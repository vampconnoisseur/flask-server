import base64
import io
import json
import os
import sys
import threading
import time
import logging
import boto3
from typing import Tuple, Dict, Any
import subprocess
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import altair as alt
from bokeh.plotting import figure
from bokeh.embed import json_item

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from jupyter_client import KernelManager
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import psutil
import re

# Import fetch_s3_file from your s3_utils module
# from s3_utils import fetch_s3_file
from queue import Empty
import uvicorn
import duckdb
from queue import Empty
from datetime import datetime, date
import ast

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("app.log")],
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SessionManager:
    def __init__(self):
        self.sessions = {}
        self.session_files = {}  # To track downloaded file paths per session
        self.lock = threading.Lock()
        self.duckdb_connections = {}
        self.dataframes = {}

    def create_session(self, session_id):
        with self.lock:
            if session_id not in self.sessions:
                try:
                    kernel = self.create_kernel()
                    self.sessions[session_id] = kernel
                    self.session_files[session_id] = (
                        []
                    )  # Initialize an empty list for tracking file paths
                    # self.duckdb_connections[session_id] = duckdb.connect(':memory:')  # Create DuckDB connection here
                    self.dataframes[session_id] = {}
                    logger.info(f"Session created: {session_id}")
                except Exception as e:
                    logger.error(f"Error creating session {session_id}: {str(e)}")
                    raise

    def get_session_dataframes(self, session_id):
        return self.dataframes.get(session_id, {})

    def add_dataframe(self, session_id, df_name, df):
        if session_id in self.dataframes:
            self.dataframes[session_id][df_name] = df

    def get_dataframe(self, session_id, df_name):
        return self.dataframes.get(session_id, {}).get(df_name)

    def get_session(self, session_id) -> Tuple[KernelManager, Any]:
        with self.lock:
            if session_id not in self.sessions:
                self.create_session(session_id)
            return self.sessions[session_id]

    def get_duckdb_connection(self, session_id):
        if session_id not in self.duckdb_connections:
            ## This should change if you want persistence
            self.duckdb_connections[session_id] = duckdb.connect(":memory:")
            # self.duckdb_connections[session_id] = duckdb.connect(f'my_database_{session_id}.duckdb')

        return self.duckdb_connections[session_id]

    def close_session(self, session_id):
        with self.lock:
            if session_id in self.sessions:
                # First, clean up the downloaded files
                # self.cleanup_files(session_id)
                kernel = self.sessions.pop(session_id)
                self.shutdown_kernel(kernel)
                logger.info(f"Session closed: {session_id}")
            if session_id in self.duckdb_connections:
                self.duckdb_connections[session_id].close()
                del self.duckdb_connections[session_id]
            if session_id in self.dataframes:
                del self.dataframes[session_id]

    def cleanup_files(self, session_id):
        # Remove all downloaded files for this session
        if session_id in self.session_files:
            for file_path in self.session_files[session_id]:
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.info(f"Removed file: {file_path}")
                    except Exception as e:
                        logger.error(f"Error removing file {file_path}: {str(e)}")
            # Clear the file list for this session
            del self.session_files[session_id]

    def restart_kernel_if_needed(self, session_id):
        """Restart the kernel if it is not alive."""
        try:
            kernel = self.get_session(session_id)
            km, kc = kernel
            if not kc.is_alive():
                logger.info(
                    f"Kernel for session {session_id} is not alive, restarting..."
                )
                self.close_session(session_id)
                self.create_session(session_id)
        except Exception as e:
            logger.error(f"Error restarting kernel for session {session_id}: {str(e)}")

    def create_kernel(self):
        try:
            km = KernelManager()
            km.start_kernel()
            kc = km.client()
            kc.start_channels()
            self.kernel_ready(kc)
            # Execute setup code in the new kernel
            setup_code = """
        import pandas as pd
        import numpy as np
        import matplotlib.pyplot as plt
        import seaborn as sns
        import plotly.graph_objects as go
        import altair as alt
        from bokeh.plotting import figure
        import duckdb
        """
            msg_id = kc.execute(setup_code)
            self.process_output(kc, msg_id)
            logger.info("Kernel created")
            return (km, kc)
        except Exception as e:
            logger.error(f"Error creating kernel: {str(e)}")
            raise

    def kernel_ready(self, kc):
        while True:
            if kc.is_alive():
                return True
            time.sleep(0.1)

    def shutdown_kernel(self, kernel):
        try:
            km, kc = kernel
            kc.stop_channels()
            km.shutdown_kernel(now=True)
            logger.info("Kernel shut down")
        except Exception as e:
            logger.error(f"Error shutting down kernel: {str(e)}")

    def process_output(self, kernel_client, msg_id):
        outputs = []
        start_time = time.time()
        timeout = 10.0  # 45 seconds timeout
        try:
            while True:
                try:
                    remaining_time = max(0, timeout - (time.time() - start_time))
                    msg = kernel_client.get_iopub_msg(timeout=remaining_time)
                    # msg = kernel_client.get_iopub_msg(timeout=30.0)
                    logger.info(f"Received message: {msg['msg_type']}")

                    if msg["parent_header"].get("msg_id") != msg_id:
                        logger.info(
                            f"Skipping message with different parent_header msg_id"
                        )
                        # Send a timeout message to the frontend
                        outputs.append(
                            (
                                "timeout_warning",
                                {
                                    "message": "Code execution is taking longer than expected. It may timeout soon."
                                },
                            )
                        )
                        continue

                    msg_type = msg["msg_type"]
                    content = msg["content"]

                    if msg_type in ["execute_result", "display_data"]:
                        data = content.get("data", {})
                        metadata = content.get("metadata", {})
                        outputs.append(
                            ("rich_output", {"data": data, "metadata": metadata})
                        )
                        logger.info(f"Appended rich_output: {data.keys()}")
                    elif msg_type == "stream":
                        outputs.append(
                            (
                                "stream",
                                {"name": content["name"], "text": content["text"]},
                            )
                        )
                        logger.info(f"Appended stream output: {content['name']}")
                    elif msg_type == "error":
                        outputs.append(
                            (
                                "error",
                                {
                                    "ename": content["ename"],
                                    "evalue": content["evalue"],
                                    "traceback": content["traceback"],
                                },
                            )
                        )
                        logger.error(
                            f"Appended error output: {content['ename']}: {content['evalue']}"
                        )
                    elif msg_type == "status" and content["execution_state"] == "idle":
                        logger.info("Kernel is idle, finishing output processing")
                        break
                except Empty:
                    logger.info("No more messages from the kernel")
                    break

                if time.time() - start_time > timeout:
                    logger.info("Execution timed out")
                    outputs.append(
                        (
                            "timeout",
                            {"message": "Code execution timed out after 45 seconds."},
                        )
                    )
                    break

        except Exception as e:
            logger.error(f"Error in process_output: {str(e)}", exc_info=True)

        logger.info(f"Processed {len(outputs)} outputs")
        return outputs


session_manager = SessionManager()


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    try:
        # Get or create the session
        session_manager.create_session(session_id)
        km, kernel_client = session_manager.get_session(session_id)

        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)

                if message["type"] == "execute_request":
                    response = await handle_execute_request(
                        kernel_client, message, session_id
                    )
                    # Use custom JSON encoder for serialization
                    json_str = json.dumps(response, default=serialize_datetime)
                    await websocket.send_text(json_str)
                # ... rest of the message handling ...
                elif message["type"] == "load_data_sources":
                    response = await handle_load_data_sources(
                        session_id, kernel_client, message
                    )
                    await websocket.send_json(response)

            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected for session {session_id}")
                break
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                # Send error to client without closing connection
                error_response = {
                    "type": "execute_response",
                    "output_type": "error",
                    "content": f"An error occurred: {str(e)}",
                    "blockId": message.get("blockId", ""),
                }
                await websocket.send_json(error_response)
                # Don't re-raise the exception - continue processing messages
                continue

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for session {session_id}")
    except Exception as e:
        logger.error(f"Error in websocket_endpoint for session {session_id}: {str(e)}")
    finally:
        session_manager.close_session(session_id)


def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):  # Add handling for date objects
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


async def handle_execute_request(kernel_client, message, session_id):
    code = message["code"]
    block_id = message["blockId"]
    runtime = message.get("runtime", "python")  # Default to Python if not specified

    session_manager.restart_kernel_if_needed(session_id)

    try:
        if runtime == "duckdb":
            # Use the globally set DuckDB connection
            duckdb_conn = session_manager.get_duckdb_connection(session_id)
            duckdb_conn.sql(f"INSTALL postgres_scanner; LOAD postgres_scanner;")
            print("Postgres scanner installed and loaded successfully.")

            result = duckdb_conn.sql(code)
            print(f"DuckDB query result: {result}")  # Print the result of the query

            # Convert the result to JSON
            rows = result.fetchall()
            columns = result.columns
            json_data = [
                {
                    col: (
                        serialize_datetime(value)
                        if isinstance(value, (datetime))
                        else value
                    )
                    for col, value in zip(columns, row)
                }
                for row in rows
            ]

            return {
                "type": "execute_response",
                "output_type": "pandas_json",
                "content": json_data,
                "blockId": block_id,
            }

        else:
            # Split code into pip commands and regular code
            pip_commands = []
            regular_code = []

            for line in code.splitlines():
                if line.strip().startswith(("!pip", "pip")):
                    pip_command = line.strip().replace("!pip", "pip")
                    pip_commands.append(pip_command)
                else:
                    regular_code.append(line)

            # Execute pip commands first
            for cmd in pip_commands:
                try:
                    result = subprocess.run(
                        cmd, shell=True, capture_output=True, text=True, check=True
                    )

                    return {
                        "type": "execute_response",
                        "output_type": "stream",
                        "content": {"text": result.stdout},
                        "blockId": block_id,
                    }

                except subprocess.CalledProcessError as e:
                    return send_error(e.stderr, block_id)

            # Execute the regular code as a single block
            if regular_code:
                code_block = "\n".join(regular_code)
                print("code_block", code_block)

                # Register DataFrames before execution
                dataframe_names = extract_dataframes_from_code(code_block)
                duckdb_conn = session_manager.get_duckdb_connection(session_id)

                # Execute the code block
                msg_id = kernel_client.execute(code_block)
                outputs = session_manager.process_output(kernel_client, msg_id)

                print("outputs", outputs)

                # Register any DataFrames after execution
                for df_name in dataframe_names:
                    try:
                        eval_msg_id = kernel_client.execute(
                            f"isinstance({df_name}, pd.DataFrame)"
                        )
                        eval_outputs = session_manager.process_output(
                            kernel_client, eval_msg_id
                        )

                        # Check if it's actually a DataFrame
                        is_df = any(
                            output_type == "rich_output"
                            and "text/plain" in content["data"]
                            and "True" in content["data"]["text/plain"]
                            for output_type, content in eval_outputs
                        )

                        if is_df:
                            # Register DataFrame in DuckDB
                            eval_msg_id = kernel_client.execute(
                                f"{df_name}.to_json(orient='records')"
                            )
                            eval_outputs = session_manager.process_output(
                                kernel_client, eval_msg_id
                            )

                            for output_type, content in eval_outputs:
                                if (
                                    output_type == "rich_output"
                                    and "text/plain" in content.get("data", {})
                                ):
                                    df_json = content["data"]["text/plain"]
                                    df_json_clean = df_json.strip("'")
                                    df = pd.read_json(df_json_clean)
                                    duckdb_conn.register(df_name, df)
                                    session_manager.add_dataframe(
                                        session_id, df_name, df
                                    )
                                    print(f"DataFrame '{df_name}' registered in DuckDB")
                    except Exception as e:
                        print(f"Error registering DataFrame {df_name}: {str(e)}")

                if not outputs:  # Check if outputs are empty

                    return {
                        "type": "execute_response",
                        "output_type": "error",
                        "content": "No output generated",
                        "blockId": block_id,
                    }

                # Handle outputs
                for output_type, content in outputs:
                    if output_type == "rich_output":
                        data = content.get("data", {})
                        print("data", data)

                        # Handle DataFrame outputs
                        if "text/html" in data and "<table" in data["text/html"]:
                            try:
                                json_msg_id = kernel_client.execute(
                                    f"{code_block}.to_json(orient='records')"
                                )
                                json_outputs = session_manager.process_output(
                                    kernel_client, json_msg_id
                                )

                                for json_output_type, json_content in json_outputs:
                                    if json_output_type == "rich_output":
                                        json_string = json_content["data"]["text/plain"]
                                        json_data = json.loads(json_string.strip("'"))

                                        return {
                                            "type": "execute_response",
                                            "output_type": "pandas_json",
                                            "content": json_data,
                                            "blockId": block_id,
                                        }

                            except Exception as e:
                                print(f"Error handling DataFrame output: {str(e)}")
                        else:

                            return {
                                "type": "execute_response",
                                "output_type": output_type,
                                "content": content,
                                "blockId": block_id,
                            }

                    else:

                        return {
                            "type": "execute_response",
                            "output_type": output_type,
                            "content": content,
                            "blockId": block_id,
                        }

    except Exception as e:
        print(f"Error in handle_execute_request: {str(e)}")
        return send_error(f"An error occurred: {str(e)}", block_id)


# Ensure this function is defined
def send_error(error_message: str, block_id: str):

    return {
        "type": "execute_response",
        "output_type": "error",
        "content": error_message,
        "blockId": block_id,
    }


# async def handle_plots(websocket: WebSocket, kernel_client, code: str, block_id: str):
#     try:
#         msg_id = kernel_client.execute(code)
#         outputs = session_manager.process_output(kernel_client, msg_id)

#         for output_type, content in outputs:
#             if output_type == "rich_output":
#                 data = content["data"]
#                 if "image/png" in data:
#                     await send_output(websocket, "image", data["image/png"], block_id)
#                 elif "application/json" in data:
#                     plot_data = json.loads(data["application/json"])
#                     await send_output(websocket, "json", plot_data, block_id)
#                 elif "text/html" in data:
#                     await send_output(websocket, "html", data["text/html"], block_id)
#             elif output_type == "error":
#                 error_message = "\n".join(content["traceback"])
#                 await send_error(websocket, error_message, block_id)
#     except Exception as e:
#         logger.error(f"Error in handle_plots: {str(e)}")
#         await send_error(
#             websocket, f"An error occurred while handling plots: {str(e)}", block_id
#         )

def extract_dataframes_from_code(code: str) -> list:
    """
    Parse the user code to identify DataFrame assignments and operations.
    Returns a list of DataFrame variable names.
    """
    try:
        tree = ast.parse(code)
        dataframe_names = set()  # Using set to avoid duplicates

        for node in ast.walk(tree):
            # Handle direct assignments (df = ...)
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        dataframe_names.add(target.id)

            # Handle method calls (df.method(...))
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    if hasattr(node.func, "value") and isinstance(
                        node.func.value, ast.Name
                    ):
                        dataframe_names.add(node.func.value.id)

            # Handle function calls with DataFrame arguments
            elif isinstance(node, ast.Call):
                for arg in node.keywords:
                    if isinstance(arg.value, ast.Name):
                        dataframe_names.add(arg.value.id)

        return list(dataframe_names)
    except Exception as e:
        print(f"Error parsing code: {str(e)}")
        return []


async def load_file_to_duckdb(
    duckdb_conn,
    session_id,
    bucket_name,
    file_key,
    aws_access_key_id,
    aws_secret_access_key,
    table_name,
    kernel_client,
    loaded_tables,
    generated_code,
):
    try:
        if file_key is not None:
            download_path, pandas_code = fetch_s3_file(
                bucket_name,
                file_key,
                aws_access_key_id,
                aws_secret_access_key,
            )
            session_manager.session_files[session_id].append(download_path)

            file_extension = os.path.splitext(file_key)[1].lower()
            file_name = os.path.basename(file_key)

            # Replace special characters (including hyphens and periods) with underscores
            file_name = re.sub(r"[^a-zA-Z0-9_]", "_", file_name)

            if file_extension == ".csv":
                pandas_code = f"{file_name} = pd.read_csv(r'''{download_path}''')"
                duckdb_conn.execute(
                    f"CREATE TABLE {file_name} AS SELECT * FROM read_csv_auto('{download_path}')"
                )
            elif file_extension == ".parquet":
                pandas_code = f"{file_name} = pd.read_parquet(r'''{download_path}''')"
                duckdb_conn.execute(
                    f"CREATE TABLE {file_name} AS SELECT * FROM parquet_scan('{download_path}')"
                )
            elif file_extension == ".json":
                pandas_code = f"{file_name} = pd.read_json(r'''{download_path}''')"
                duckdb_conn.execute(
                    f"CREATE TABLE {file_name} AS SELECT * FROM read_json_auto('{download_path}')"
                )
            elif file_extension in [".xls", ".xlsx"]:
                print(file_name)
                pandas_code = f"{file_name} = pd.read_excel(r'''{download_path}''')"
                duckdb_conn.execute(
                    f"CREATE TABLE {file_name} AS SELECT * FROM st_read('{download_path}')"
                )
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")

            pandas_load_code = f"{pandas_code}"
            generated_code.append(pandas_load_code)

            print(pandas_load_code)
            # Execute the Pandas load code
            msg_id = kernel_client.execute(pandas_load_code)
            session_manager.process_output(kernel_client, msg_id)

            loaded_tables.append(file_name)
            logger.info(f"Loaded {file_key} into Pandas and made available in DuckDB")
        else:
            return send_error(f"Failed to fetch the S3 file: {file_key}", None)
    except Exception as e:
        logger.error(f"Error loading file: {str(e)}")
        return send_error(f"Error loading file: {str(e)}", None)


async def handle_load_data_sources(session_id, kernel_client, message):
    try:
        data_sources = message.get("dataSources", [])
        logger.info(f"Received data sources: {data_sources}")

        if not data_sources:

            return {
                "type": "load_data_sources_response",
                "status": "No data sources provided",
                "tables": [],
            }

        # session_id = websocket.url.path.split("/")[-1]
        duckdb_conn = session_manager.get_duckdb_connection(session_id)
        loaded_tables = []
        generated_code = []

        for source in data_sources:
            source_type = source.get("type")
            table_name = source.get("name")
            logger.info(f"Processing source: {source_type}, {table_name}")

            if source_type == "file":
                bucket_name, aws_access_key_id, aws_secret_access_key = (
                    await get_s3_conn_details_hardcoded()
                )
                file_key = source.get("file_path")

                await load_file_to_duckdb(
                    duckdb_conn,
                    session_id,
                    bucket_name,
                    file_key,
                    aws_access_key_id,
                    aws_secret_access_key,
                    table_name,
                    kernel_client,
                    loaded_tables,
                    generated_code,
                )

            elif source_type == "database":
                connection_string = source.get("connection_string")
                schema_name = source.get("name")
                tables = source.get("tables", [])

                for table_name in tables:
                    table_name = table_name.replace("-", "_")
                    try:
                        if connection_string.startswith(("postgresql://", "mysql://")):
                            # Load into Pandas
                            pandas_load_code = f"""
                            import sqlalchemy
                            engine = sqlalchemy.create_engine(r'''{connection_string}''')
                            {table_name} = pd.read_sql_table(r'''{table_name}''', engine, schema=r'''{schema_name}''')
                            """
                            # generated_code.append(pandas_load_code)
                            # msg_id = kernel_client.execute(pandas_load_code)
                            # session_manager.process_output(kernel_client, msg_id)

                            # Make available in DuckDB
                            # duckdb_load_code = f"duckdb_conn.register('{table_name}', {table_name})"
                            # msg_id = kernel_client.execute(duckdb_load_code)
                            # session_manager.process_output(kernel_client, msg_id)

                            postgres_scan_query = f"""
                            INSTALL postgres_scanner;
                            LOAD postgres_scanner;
                            CREATE TABLE {table_name} AS SELECT * FROM postgres_scan(
                            '{connection_string}', 'public', '{table_name}'
                            );
                            """
                            duckdb_conn.execute(postgres_scan_query)
                            logger.info(
                                f"Loaded table {table_name} from PostgreSQL database into DuckDB."
                            )
                            loaded_tables.append(table_name)

                        elif connection_string.startswith("mongodb://"):
                            # Load into Pandas
                            pandas_load_code = f"""
                            from pymongo import MongoClient
                            client = MongoClient(r'''{connection_string}''')
                            db = client.get_database()
                            {table_name} = pd.DataFrame(list(db.{schema_name}.{table_name}.find()))
                            """
                            generated_code.append(pandas_load_code)
                            msg_id = kernel_client.execute(pandas_load_code)
                            session_manager.process_output(kernel_client, msg_id)

                            # Make available in DuckDB
                            duckdb_load_code = (
                                f"duckdb_conn.register('{table_name}', {table_name})"
                            )
                            msg_id = kernel_client.execute(duckdb_load_code)
                            session_manager.process_output(kernel_client, msg_id)

                        else:
                            raise ValueError(
                                f"Unsupported database type: {connection_string}"
                            )

                        loaded_tables.append(table_name)
                        logger.info(
                            f"Loaded database table into Pandas and made available in DuckDB: {table_name}"
                        )
                    except Exception as e:
                        logger.error(f"Error loading database table: {str(e)}")
                        continue
                        # return send_error(
                        #     f"Error loading database table: {str(e)}", None
                        # )

            elif source_type == "cloud":
                connection_string = source.get("connection_string")
                schema_name = source.get("name")
                tables = source.get("tables", [])
                conn_details = json.loads(connection_string)
                bucket_name = conn_details["bucket"]
                aws_access_key_id = conn_details["access_key_id"]
                aws_secret_access_key = conn_details["secret_access_key"]

                for table_name in tables:
                    logger.info(f"Loading table from cloud: {table_name}")
                    file_key = (
                        table_name  # Assuming file_key is provided for cloud source
                    )

                    await load_file_to_duckdb(
                        duckdb_conn,
                        session_id,
                        bucket_name,
                        file_key,
                        aws_access_key_id,
                        aws_secret_access_key,
                        table_name,
                        kernel_client,
                        loaded_tables,
                        generated_code,
                    )

        return {
            "type": "load_data_sources_response",
            "status": "Data sources loaded successfully into Pandas and made available in DuckDB!",
            "tables": loaded_tables,
            "code": generated_code,
        }

    except Exception as e:
        logger.error(f"Error in handle_load_data_sources: {str(e)}")
        return send_error(
            f"An error occurred while loading data sources: {str(e)}", None
        )


def fetch_s3_file(bucket_name, file_key, aws_access_key_id, aws_secret_access_key):
    """
    Fetches a file from the specified S3 bucket, loads it into both Pandas and DuckDB.

    :param bucket_name: Name of the S3 bucket
    :param file_key: Key of the file in the S3 bucket
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param session_manager: The session manager that holds the DuckDB connection
    :param session_id: The session ID to get the DuckDB connection
    :param table_name: The name of the table to register the data in DuckDB
    :return: A tuple of download_path, pandas dataframe (as pandas code string), and success message for DuckDB
    """
    try:
        # Create an S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        # Define the local path to download the file
        download_path = os.path.join(os.getcwd(), file_key.split("/")[-1])

        # Download the file from S3
        s3.download_file(bucket_name, file_key, download_path)
        print(f"File downloaded successfully to {download_path}")

        # Initialize an empty variable to store the Pandas code string
        pandas_code = ""

        # retreive the session_id for this session
        session_id = session_manager.get_session

        # Get the DuckDB connection for this session
        duckdb_conn = session_manager.get_duckdb_connection(session_id)

        # Load into Pandas and register in DuckDB based on file extension
        file_extension = os.path.splitext(file_key)[1].lower()
        file_name = os.path.basename(file_key)

        # Replace special characters (including hyphens and periods) with underscores
        file_name = re.sub(r"[^a-zA-Z0-9_]", "_", file_name)

        # if file_extension == '.csv':
        #     pandas_code = f"{file_name} = pd.read_csv(r'''{download_path}''')"
        #     duckdb_conn.execute(f"CREATE TABLE {file_name} AS SELECT * FROM read_csv_auto('{download_path}')")
        # elif file_extension == '.parquet':
        #     pandas_code = f"{file_name} = pd.read_parquet(r'''{download_path}''')"
        #     duckdb_conn.execute(f"CREATE TABLE {file_name} AS SELECT * FROM parquet_scan('{download_path}')")
        # elif file_extension == '.json':
        #     pandas_code = f"{file_name} = pd.read_json(r'''{download_path}''')"
        #     duckdb_conn.execute(f"CREATE TABLE {file_name} AS SELECT * FROM read_json_auto('{download_path}')")
        # elif file_extension in ['.xls', '.xlsx']:
        #     pandas_code = f"{file_name} = pd.read_excel(r'''{download_path}''')"
        #     duckdb_conn.execute(f"CREATE TABLE {file_name} AS SELECT * FROM read_excel('{download_path}')")
        # else:
        #     raise ValueError(f"Unsupported file type: {file_extension}")

        # print(f"Table '{file_name}' successfully created in DuckDB from the file '{download_path}'")

        # Return both the pandas code (as a string) and the success message from DuckDB
        return download_path, pandas_code

    except Exception as e:
        print(f"Failed to fetch the file and register it in DuckDB and Pandas: {e}")
        raise


def extract_variable_from_code(code: str, variable_name: str) -> str:
    pattern = rf'{variable_name}\s*=\s*[\'"](.+?)[\'"]'
    match = re.search(pattern, code)
    if match:
        return match.group(1)
    raise ValueError(f"Could not extract {variable_name} from the code")


async def send_output(output_type: str, content: Any, block_id: str):
    message = {
        "type": "execute_response",
        "output_type": output_type,
        "content": content,
        "blockId": block_id,
    }
    return message


def send_error(error_message: str, block_id: str):
    message = {
        "type": "execute_response",
        "output_type": "error",
        "content": error_message,
        "blockId": block_id,
    }
    return message


@app.get("/test")
async def test_route():
    return {"message": "My route is working fine!"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8765)
