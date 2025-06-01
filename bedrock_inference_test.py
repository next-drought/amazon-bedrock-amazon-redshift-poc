import boto3
import json
from botocore.exceptions import ClientError

def test_bedrock_inference():
    try:
        # Initialize bedrock-runtime client
        bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        # Simple prompt
        prompt = "Explain AWS Bedrock in one sentence"
        
        # Titan text model request
        body = json.dumps({
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 100,
                "temperature": 0.7
            }
        })
        
        response = bedrock.invoke_model(
            modelId="amazon.titan-text-express-v1",
            body=body
        )
        
        result = json.loads(response['body'].read())
        print("Inference successful!")
        print(f"Response: {result['results'][0]['outputText']}")
        return True
        
    except ClientError as e:
        print(f"Inference error: {e}")
        if e.response['Error']['Code'] == 'AccessDeniedException':
            print("\nPossible IAM permission issue. Ensure your IAM role has:")
            print("- bedrock:InvokeModel permission")
            print("- Or AmazonBedrockFullAccess policy attached")
        return False

if __name__ == "__main__":
    test_bedrock_inference()