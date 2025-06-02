import os
from dotenv import load_dotenv
import yaml
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain.prompts.prompt import PromptTemplate
from langchain.sql_database import SQLDatabase
from langchain.chains.sql_database.prompt import PROMPT_SUFFIX, _postgres_prompt
from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from langchain.llms import Bedrock
from langchain.prompts.example_selector.semantic_similarity import (
    SemanticSimilarityExampleSelector,
)
from langchain_community.vectorstores import Chroma
from langchain_experimental.sql import SQLDatabaseChain

# Loading environment variables
load_dotenv()

# configuring your instance of Amazon bedrock, selecting the CLI profile, modelID, endpoint url and region.
llm = Bedrock(
    credentials_profile_name=os.getenv("profile_name", "default"),
    model_id="amazon.titan-text-express-v1",
    endpoint_url="https://bedrock-runtime.us-east-1.amazonaws.com",
    region_name="us-east-1",
    verbose=True
)


def redshift_answer(question):
    """
    This function collects all necessary information to execute the sql_db_chain and get an answer generated, taking
    a natural language question in and returning an answer and generated SQL query.
    :param question: The question the user passes in from the frontend
    :return: The final answer in natural language along with the generated SQL query.
    """
    try:
        # retrieving the final Redshift URI to initiate a connection with the database
        redshift_uri = get_redshift_uri()
        print(f"Connecting to Redshift with URI: {redshift_uri.replace(os.getenv('REDSHIFT_PASSWORD', ''), '***')}")
        
        # formatting the Redshift URI and preparing it to be used with Langchain sql_db_chain
        db = SQLDatabase.from_uri(redshift_uri)
        
        # loading the sample prompts from SampleData/moma_examples.yaml
        examples = load_samples()
        
        # initiating the sql_db_chain with the specific LLM we are using, the db connection string and the selected examples
        sql_db_chain = load_few_shot_chain(llm, db, examples)
        
        # the answer created by Amazon Bedrock and ultimately passed back to the end user
        answer = sql_db_chain(question)
        
        # Passing back both the generated SQL query and the final result in a natural language format
        return answer["intermediate_steps"][1], answer["result"]
        
    except Exception as e:
        print(f"Error in redshift_answer: {str(e)}")
        return f"-- Error: {str(e)}", f"Sorry, I encountered an error: {str(e)}"


def get_redshift_uri():
    """
    This function is used to build the Redshift URL and eventually used to connect to the database.
    :return: The full Redshift URL that is used to query against.
    """
    # setting the key parameters to build the Redshift connection string, these are stored in the .env file
    # Updated to match common environment variable naming patterns
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST') or os.getenv('redshift_host')
    REDSHIFT_PORT = os.getenv('REDSHIFT_PORT') or os.getenv('redshift_port') or '5439'
    REDSHIFT_DATABASE = os.getenv('REDSHIFT_DB') or os.getenv('redshift_database')
    REDSHIFT_USERNAME = os.getenv('REDSHIFT_USER') or os.getenv('redshift_username')
    REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD') or os.getenv('redshift_password')

    # Clean up the port - ensure it's not None or empty string
    if not REDSHIFT_PORT or REDSHIFT_PORT.lower() == 'none':
        REDSHIFT_PORT = '5439'
    
    # Validate that all required parameters are present
    if not all([REDSHIFT_HOST, REDSHIFT_DATABASE, REDSHIFT_USERNAME, REDSHIFT_PASSWORD]):
        missing = [var for var, val in {
            'REDSHIFT_HOST': REDSHIFT_HOST,
            'REDSHIFT_DATABASE': REDSHIFT_DATABASE, 
            'REDSHIFT_USERNAME': REDSHIFT_USERNAME,
            'REDSHIFT_PASSWORD': REDSHIFT_PASSWORD
        }.items() if not val]
        raise ValueError(f"Missing required environment variables: {missing}")

    print(f"Debug - Connection params: Host={REDSHIFT_HOST}, Port={REDSHIFT_PORT}, DB={REDSHIFT_DATABASE}, User={REDSHIFT_USERNAME}")

    # taking all the inputted parameters and formatting them in a finalized string
    REDSHIFT_ENDPOINT = f"redshift+psycopg2://{REDSHIFT_USERNAME}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DATABASE}"
    
    # returning the final Redshift URL that was built in the line of code above
    return REDSHIFT_ENDPOINT


def load_samples():
    """
    Load the sql examples for few-shot prompting examples
    :return: The sql samples from the moma_examples.yaml file
    """
    try:
        # Fixed the file path case sensitivity
        yaml_path = "SampleData/moma_examples.yaml"
        if not os.path.exists(yaml_path):
            print(f"Warning: {yaml_path} not found, trying alternative paths...")
            # Try some alternative paths
            alternative_paths = [
                "Sampledata/moma_examples.yaml",
                "sampledata/moma_examples.yaml", 
                "moma_examples.yaml"
            ]
            for alt_path in alternative_paths:
                if os.path.exists(alt_path):
                    yaml_path = alt_path
                    print(f"Found YAML file at: {yaml_path}")
                    break
            else:
                raise FileNotFoundError(f"Could not find moma_examples.yaml in any expected location")
        
        # opening our prompt sample file
        with open(yaml_path, "r") as stream:
            # reading our prompt samples into the sql_samples variable
            sql_samples = yaml.safe_load(stream)
        
        print(f"Loaded {len(sql_samples)} examples from {yaml_path}")
        return sql_samples
        
    except Exception as e:
        print(f"Error loading samples: {str(e)}")
        # Return a basic example if file loading fails
        return [{
            "input": "How many artists are there?",
            "sql_cmd": "SELECT COUNT(*) FROM artists;",
            "sql_result": "[(15086,)]",
            "answer": "There are 15086 artists in the database.",
            "table_info": """CREATE TABLE artists (
                artist_id INTEGER NOT NULL,
                full_name VARCHAR(200),
                nationality VARCHAR(50),
                gender VARCHAR(25),
                birth_year INTEGER,
                death_year INTEGER,
                CONSTRAINT artists_pk PRIMARY KEY (artist_id)
            )"""
        }]


def load_few_shot_chain(llm, db, examples):
    """
    This function is used to load in the most similar prompts, format them along with the users question and then is
    passed in to Amazon Bedrock to generate an answer.
    :param llm: Large Language model you are using
    :param db: The Redshift database URL
    :param examples: The samples loaded from your examples file.
    :return: The results from the SQLDatabaseChain
    """
    try:
        # This is formatting the prompts that are retrieved from the SampleData/moma_examples.yaml
        example_prompt = PromptTemplate(
            input_variables=["table_info", "input", "sql_cmd", "sql_result", "answer"],
            template=(
                "{table_info}\n\nQuestion: {input}\nSQLQuery: {sql_cmd}\nSQLResult:"
                " {sql_result}\nAnswer: {answer}"
            ),
        )
        
        # instantiating the hugging face embeddings model to be used to produce embeddings of user queries and prompts
        local_embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
        
        # The example selector loads the examples, creates the embeddings, stores them in Chroma (vector store) and a
        # semantic search is performed to see the similarity between the question and prompts, it returns the 3 most similar
        # prompts as defined by k
        example_selector = SemanticSimilarityExampleSelector.from_examples(
            examples,
            local_embeddings,
            Chroma,
            k=min(3, len(examples)),
        )
        
        # This is orchestrating the example selector (finding similar prompts to the question), example_prompt (formatting
        # the retrieved prompts, and formatting the chat history and the user input
        few_shot_prompt = FewShotPromptTemplate(
            example_selector=example_selector,
            example_prompt=example_prompt,
            prefix=_postgres_prompt + "Provide no preamble",
            suffix=PROMPT_SUFFIX,
            input_variables=["table_info", "input", "top_k"],
        )
        
        # Where the LLM, DB and prompts are all orchestrated to answer a user query.
        return SQLDatabaseChain.from_llm(
            llm,
            db,
            prompt=few_shot_prompt,
            use_query_checker=True,
            verbose=True,
            return_intermediate_steps=True,
        )
    except Exception as e:
        print(f"Error creating few-shot chain: {str(e)}")
        raise


def test_redshift_connection():
    """Test the Redshift connection and basic functionality"""
    try:
        redshift_uri = get_redshift_uri()
        db = SQLDatabase.from_uri(redshift_uri)
        
        # Test basic connection
        result = db.run("SELECT 1")
        print(f"✓ Database connection successful")
        
        # Test artists table
        result = db.run("SELECT COUNT(*) FROM artists")
        print(f"✓ Artists table accessible, count: {result}")
        
        return True
    except Exception as e:
        print(f"✗ Connection test failed: {str(e)}")
        return False


if __name__ == "__main__":
    print("Testing Redshift connection and LangChain setup...")
    
    if test_redshift_connection():
        print("\nTesting sample question...")
        try:
            sql, answer = redshift_answer("How many artists are there?")
            print(f"SQL: {sql}")
            print(f"Answer: {answer}")
        except Exception as e:
            print(f"Error testing sample question: {str(e)}")
    else:
        print("Cannot proceed with tests due to connection issues.")