import os
import json
import boto3
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Loading environment variables
load_dotenv()

def call_bedrock_directly(prompt, model_id="amazon.titan-text-express-v1"):
    """Call AWS Bedrock directly without LangChain"""
    try:
        bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        body = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 500,
                "temperature": 0.1,
                "topP": 0.9
            }
        }
        
        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json"
        )
        
        response_body = json.loads(response['body'].read())
        return response_body['results'][0]['outputText'].strip()
        
    except Exception as e:
        print(f"Bedrock error: {e}")
        return None

def get_redshift_connection():
    """Create Redshift connection"""
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
    REDSHIFT_PORT = os.getenv('REDSHIFT_PORT') or '5439'
    REDSHIFT_DATABASE = os.getenv('REDSHIFT_DB')
    REDSHIFT_USERNAME = os.getenv('REDSHIFT_USER')
    REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')

    if not REDSHIFT_PORT or REDSHIFT_PORT.lower() == 'none':
        REDSHIFT_PORT = '5439'

    if not all([REDSHIFT_HOST, REDSHIFT_DATABASE, REDSHIFT_USERNAME, REDSHIFT_PASSWORD]):
        raise ValueError("Missing required Redshift environment variables")

    connection_string = f"redshift+psycopg2://{REDSHIFT_USERNAME}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DATABASE}"
    return create_engine(connection_string, isolation_level="AUTOCOMMIT")

def get_table_schema():
    """Return database schema for prompt"""
    return """
Database Schema:
CREATE TABLE artists (
    artist_id INTEGER NOT NULL,
    full_name VARCHAR(200),
    nationality VARCHAR(50), 
    gender VARCHAR(25),
    birth_year INTEGER,
    death_year INTEGER,
    CONSTRAINT artists_pk PRIMARY KEY (artist_id)
)

Sample data:
artist_id | full_name      | nationality | gender | birth_year | death_year
1         | Robert Arneson | American    | Male   | 1930       | 1992
2         | Doroteo Arnaiz | Spanish     | Male   | 1936       | NULL
3         | Bill Arnold    | American    | Male   | 1941       | NULL

Examples:
- "How many artists are there?" → SELECT COUNT(*) FROM artists;
- "How many French artists?" → SELECT COUNT(*) FROM artists WHERE nationality = 'French';
- "Show top nationalities" → SELECT nationality, COUNT(*) FROM artists GROUP BY nationality ORDER BY COUNT(*) DESC LIMIT 10;
"""

def extract_sql_from_response(response):
    """Extract SQL from LLM response"""
    if not response:
        return None
    
    # Look for SQL in code blocks
    if "```sql" in response:
        start = response.find("```sql") + 6
        end = response.find("```", start)
        return response[start:end].strip()
    elif "```" in response:
        start = response.find("```") + 3
        end = response.find("```", start)
        return response[start:end].strip()
    
    # Look for SELECT statements
    lines = response.split('\n')
    sql_lines = []
    for line in lines:
        if any(keyword in line.upper() for keyword in ['SELECT', 'FROM', 'WHERE']):
            sql_lines.append(line.strip())
    
    return '\n'.join(sql_lines) if sql_lines else response.strip()

def simple_nl_to_sql(question):
    """Simple pattern matching for common queries"""
    question_lower = question.lower()
    
    if "how many" in question_lower and "artist" in question_lower:
        if "french" in question_lower:
            return "SELECT COUNT(*) FROM artists WHERE nationality = 'French';"
        elif "american" in question_lower:
            return "SELECT COUNT(*) FROM artists WHERE nationality = 'American';"
        else:
            return "SELECT COUNT(*) FROM artists;"
    
    elif "nationality" in question_lower and ("count" in question_lower or "group" in question_lower):
        return """SELECT nationality, COUNT(*) as count 
                  FROM artists 
                  WHERE nationality IS NOT NULL 
                  GROUP BY nationality 
                  ORDER BY count DESC 
                  LIMIT 10;"""
    
    elif "gender" in question_lower and ("count" in question_lower or "group" in question_lower):
        return """SELECT gender, COUNT(*) as count 
                  FROM artists 
                  WHERE gender IS NOT NULL 
                  GROUP BY gender 
                  ORDER BY count DESC;"""
    
    elif any(word in question_lower for word in ["show", "list", "display"]):
        return "SELECT artist_id, full_name, nationality, gender, birth_year, death_year FROM artists LIMIT 10;"
    
    else:
        return "SELECT COUNT(*) FROM artists;"

def natural_language_to_sql(question):
    """Convert natural language to SQL using Bedrock LLM only"""
    schema = get_table_schema()
    prompt = f"""Convert this natural language question to SQL using the schema below.
Return ONLY the SQL query, no explanations.

{schema}

Question: {question}

SQL:"""
    
    response = call_bedrock_directly(prompt)
    if not response:
        raise Exception("Bedrock LLM failed to respond")
    
    sql = extract_sql_from_response(response)
    if not sql or 'SELECT' not in sql.upper():
        raise Exception("Invalid SQL generated by LLM")
    
    return sql

def execute_sql_query(sql_query):
    """Execute SQL and format results"""
    try:
        engine = get_redshift_connection()
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
            
            if not rows:
                return "No results found."
            
            # Single value result
            if len(rows) == 1 and len(rows[0]) == 1:
                return f"{rows[0][0]:,}"
            
            # Multiple rows - format as table
            formatted_result = []
            
            # Add rows (limit to 20)
            for i, row in enumerate(rows[:20]):
                row_str = " | ".join(f"{str(val):15}" for val in row)
                formatted_result.append(row_str)
            
            if len(rows) > 20:
                formatted_result.append(f"... and {len(rows) - 20} more rows")
            
            return "\n".join(formatted_result)
            
    except Exception as e:
        return f"Error executing query: {str(e)}"

def redshift_answer(question):
    """Main function - returns (sql, answer) tuple"""
    try:
        print(f"Processing: {question}")
        
        # Convert to SQL
        sql_query = natural_language_to_sql(question)
        
        if not sql_query:
            return ("-- Unable to generate SQL", "Sorry, I couldn't understand your question.")
        
        print(f"Generated SQL: {sql_query}")
        
        # Execute SQL
        answer = execute_sql_query(sql_query)
        
        return (sql_query, answer)
        
    except Exception as e:
        error_msg = f"Sorry, I encountered an error: {str(e)}"
        return ("-- Error occurred", error_msg)

def test_connection():
    """Test the setup"""
    try:
        # Test Bedrock
        response = call_bedrock_directly("Hello, can you help me?")
        print(f"✓ Bedrock test: {response[:50] if response else 'Failed'}")
        
        # Test Redshift
        engine = get_redshift_connection()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM artists"))
            count = result.fetchone()[0]
            print(f"✓ Redshift test: {count} artists found")
        
        # Test full flow
        sql, answer = redshift_answer("How many artists are there?")
        print(f"✓ Full test - SQL: {sql}")
        print(f"✓ Full test - Answer: {answer}")
        
        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()