import asyncio
from pydantic import Field
from langchain_core.tools import BaseTool
from pydantic import SecretStr
from typing import TypedDict, Annotated
import logging
from psycopg_pool.pool_async import AsyncConnectionPool
from langgraph.prebuilt import (
    ToolNode,
    tools_condition,
)
from langgraph.graph import StateGraph, add_messages, START, END
from langgraph.graph.state import CompiledStateGraph, RunnableConfig
from langchain_openai.chat_models import ChatOpenAI
from langchain_core.messages import HumanMessage, BaseMessage
from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
import os
from psycopg import AsyncConnection
from psycopg.rows import TupleRow


class DatabaseState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]


async def stream_graph_updates(
    graph: CompiledStateGraph,
    user_input: str,
    config: RunnableConfig | None = None,
):
    events = graph.astream(
        input={"messages": [HumanMessage(user_input)]},
        config=config,
        stream_mode="values",
    )
    async for event in events:
        event["messages"][-1].pretty_print()


class SQLTools:
    def __init__(self, connection_pool: AsyncConnectionPool):
        self.connection_pool = connection_pool

    async def run_sql_tool(
        self, query: str = Field(description="The SQL query to be executed")
    ):
        """Runs a SQL query against the connected Postgres database."""
        async with self.connection_pool.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(query)
                if cursor.description:
                    return cursor.fetchall()
                await db.commit()
                return "Query executed successfully"

    async def get_current_database_schema_tool(self):
        """Returns the current database schema."""
        async with self.connection_pool.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position;
                """
                )
                schema = await cursor.fetchall()

        schema_dict = {}
        for table_name, column_name, data_type in schema:
            if table_name not in schema_dict:
                schema_dict[table_name] = []
            schema_dict[table_name].append(
                {"column_name": column_name, "data_type": data_type}
            )

        return schema_dict


def create_call_llm_node(llm: BaseChatModel, tools: list[BaseTool]):
    llm_with_tools = llm.bind_tools(tools)

    def call_llm_node(state: DatabaseState) -> DatabaseState:
        result = llm_with_tools.invoke(state["messages"])
        return {
            "messages": [result],
        }

    return call_llm_node

async def main():
    connection_pool = AsyncConnectionPool[AsyncConnection[TupleRow]](
        conninfo=os.getenv("DATABASE_URL", ""),
        kwargs={"autocommit": True, "prepare_threshold": 0},
    )
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        api_key=SecretStr(os.getenv("OPENAI_API_KEY", "")),
    )
    sql_tools = SQLTools(connection_pool)
    tools = [
        sql_tools.run_sql_tool,
        sql_tools.get_current_database_schema_tool,
    ]

    tools_node = ToolNode(
        tools=[
            sql_tools.run_sql_tool,
            sql_tools.get_current_database_schema_tool,
        ]
    )
    builder = StateGraph(DatabaseState)
    builder.add_node("database_node", create_call_llm_node(llm, tools))
    builder.add_node("tools", tools_node)

    builder.add_edge(START, "database_node")
    builder.add_conditional_edges("database_node", tools_condition)
    builder.add_edge("tools", "database_node")
    builder.add_edge("database_node", END)

    async with (
        AsyncPostgresSaver.from_conn_string(
            os.getenv("DATABASE_URL", "")
        ) as checkpointer,
    ):
        await checkpointer.setup()
        graph = builder.compile(checkpointer=checkpointer)
        try:
            while True:
                user_input = input("User: ")
                if user_input.lower() in ["quit", "exit", "q"]:
                    print("bye")
                    break

                await stream_graph_updates(
                    graph, user_input, {"configurable": {"thread_id": "14"}}
                )
        except Exception:
            logging.exception("An error occurred")


if __name__ == "__main__":
    asyncio.run(main())