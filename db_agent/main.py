from dotenv import load_dotenv
from typing import TypedDict, Annotated
import json
import logging
from psycopg_pool import ConnectionPool
from langgraph.prebuilt import (
    ToolNode,
    tools_condition,
)
from langgraph.graph import StateGraph, add_messages, START, END
from langgraph.graph.state import CompiledStateGraph, RunnableConfig
from langchain_openai.chat_models import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, ToolMessage, BaseMessage
from langgraph.checkpoint.postgres import PostgresSaver
import os


load_dotenv()

connection_pool = ConnectionPool(
    conninfo=os.getenv("DATABASE_URL", ""),
    kwargs={"autocommit": True, "prepare_threshold": 0},
)


class DatabaseState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    database_schema: Annotated[dict, "The current database schema"]


def stream_graph_updates(
    graph: CompiledStateGraph,
    user_input: str,
    config: RunnableConfig | None = None,
):
    events = graph.stream(
        input={"messages": [HumanMessage(user_input)]},
        config=config,
        stream_mode="values",
    )
    for event in events:
        event["messages"][-1].pretty_print()


@tool
def run_sql_tool(query: str):
    """Runs a SQL query against the connected Postgres database."""
    with connection_pool.connection() as db:
        with db.cursor() as cursor:
            cursor.execute(query)  # type: ignore
            if cursor.description:
                return cursor.fetchall()
            db.commit()
            return "Query executed successfully"


@tool
def get_current_database_schema_tool():
    """Returns the current database schema."""
    with connection_pool.connection() as db:
        with db.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """
            )
            schema = cursor.fetchall()

    schema_dict = {}
    for table_name, column_name, data_type in schema:
        if table_name not in schema_dict:
            schema_dict[table_name] = []
        schema_dict[table_name].append(
            {"column_name": column_name, "data_type": data_type}
        )

    return schema_dict


def create_database_node(llm: ChatOpenAI):
    llm_with_tools = llm.bind_tools([run_sql_tool, get_current_database_schema_tool])

    def database_node(state: DatabaseState) -> DatabaseState:
        for message in reversed(state["messages"]):
            if (
                isinstance(message, ToolMessage)
                and message.name == get_current_database_schema_tool.name
            ):
                state["database_schema"] = json.loads(str(message.content))
                break

        result = llm_with_tools.invoke(state["messages"])
        return {
            "messages": [result],
            "database_schema": state.get("database_schema", {}),
        }

    return database_node


if __name__ == "__main__":
    llm = ChatOpenAI(model="gpt-4o-mini")

    tools_node = ToolNode(tools=[run_sql_tool, get_current_database_schema_tool])
    builder = StateGraph(DatabaseState)
    builder.add_node("database_node", create_database_node(llm))
    builder.add_node("tools", tools_node)

    builder.add_edge(START, "database_node")
    builder.add_conditional_edges("database_node", tools_condition)
    builder.add_edge("tools", "database_node")
    builder.add_edge("database_node", END)

    with connection_pool.connection() as db:
        checkpointer = PostgresSaver(conn=db)

        checkpointer.setup()

        graph = builder.compile(checkpointer=checkpointer)
        try:
            while True:
                user_input = input("User: ")
                if user_input.lower() in ["quit", "exit", "q"]:
                    print("bye")
                    break

                stream_graph_updates(
                    graph, user_input, {"configurable": {"thread_id": "14"}}
                )
        except Exception:
            logging.exception("An error occurred")
