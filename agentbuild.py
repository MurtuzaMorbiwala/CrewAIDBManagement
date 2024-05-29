from langchain import LangGraph, LangChain, PromptTemplate, OpenAI
from langchain.tools import SQLDatabaseTool
from langchain.sql_database import SQLiteDatabase

# Initialize the SQLite database
db = SQLiteDatabase.from_uri("sqlite:///DimensionalModel.db")
db_tool = SQLDatabaseTool(db=db)

# Define the Architect Agent
llm = OpenAI(temperature=0)
architect_template = """
Requirements: {requirements}
Source Data Model: {source_data_model}

Based on the provided requirements and source data model, generate SQL statements to create or update tables in the target database.
"""
architect_prompt = PromptTemplate(input_variables=["requirements", "source_data_model"], template=architect_template)

architect_chain = LangChain(llm=llm, prompt=architect_prompt, output_keys=["sql_statements"])

# Define the Database Agent
database_template = """
SQL Statements: {sql_statements}
Execute the provided SQL statements on the target database and report any errors or successful execution.
"""
database_prompt = PromptTemplate(input_variables=["sql_statements"], template=database_template)

database_chain = LangChain(llm=llm, prompt=database_prompt, tools=[db_tool], output_keys=["result"])

# Create the LangGraph
lang_graph = LangGraph(architect_chain, database_chain, graph_name="Architect-Database Interaction", verbose=True)

# Function to handle the interaction
def handle_requirements(requirements, source_data_model):
    return lang_graph.run({"requirements": requirements, "source_data_model": source_data_model})

# Example usage
requirements = "Create a fact table for sales transactions and related dimension tables for products, customers, and dates."
source_data_model = "The source data includes CSV files with sales transactions, product information, customer data, and date details."

response = handle_requirements(requirements, source_data_model)
print(response)