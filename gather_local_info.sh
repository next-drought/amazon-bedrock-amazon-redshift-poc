#!/bin/bash

echo "🔍 GATHERING LOCAL MACHINE & AWS DEPLOYMENT INFO"
echo "================================================="
echo "Run this from your LOCAL machine (not EC2)"
echo ""

# Create output file
OUTPUT_FILE="local_deployment_info.txt"
echo "📝 Saving output to: $OUTPUT_FILE"
echo ""

{
echo "DEPLOYMENT INFO GATHERED: $(date)"
echo "=========================================="
echo ""

echo "🏠 LOCAL MACHINE INFO:"
echo "----------------------"
echo "Operating System: $(uname -a)"
echo "Python Version: $(python --version 2>&1 || python3 --version 2>&1)"
echo "Python Path: $(which python || which python3)"
echo "Current Directory: $(pwd)"
echo "User: $(whoami)"
echo ""

echo "☁️ AWS CLI INFO:"
echo "----------------"
echo "AWS CLI Version: $(aws --version 2>&1)"
echo "AWS Region: $(aws configure get region 2>&1)"
echo "AWS Profile: $(aws configure get profile 2>&1 || echo 'default')"
echo ""

echo "🔐 AWS IDENTITY:"
echo "----------------"
aws sts get-caller-identity 2>&1
echo ""

echo "🔑 KEY PAIRS:"
echo "-------------"
echo "Available Key Pairs:"
aws ec2 describe-key-pairs --query "KeyPairs[].KeyName" --output text 2>&1
echo ""
echo "Local SSH Keys:"
ls -la ~/.ssh/*.pem 2>/dev/null || echo "No .pem files found in ~/.ssh/"
echo ""

echo "🖥️ RUNNING EC2 INSTANCES:"
echo "------------------------"
aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].[InstanceId,InstanceType,PublicIpAddress,PrivateIpAddress,KeyName,LaunchTime]" \
  --output table 2>&1
echo ""

echo "🏗️ IAM ROLES:"
echo "-------------"
echo "Existing relevant IAM roles:"
aws iam list-roles \
  --query "Roles[?contains(RoleName,'Bedrock') || contains(RoleName,'Redshift') || contains(RoleName,'EC2') || contains(RoleName,'Streamlit')].[RoleName,Arn]" \
  --output table 2>&1
echo ""

echo "🔒 SECURITY GROUPS:"
echo "------------------"
aws ec2 describe-security-groups \
  --query "SecurityGroups[?GroupName!='default'].[GroupName,GroupId,Description]" \
  --output table 2>&1
echo ""

echo "🌐 NETWORK INFO:"
echo "---------------"
echo "Default VPC:"
aws ec2 describe-vpcs \
  --filters "Name=is-default,Values=true" \
  --query "Vpcs[].[VpcId,CidrBlock,State]" \
  --output table 2>&1
echo ""

echo "Default Subnets:"
aws ec2 describe-subnets \
  --filters "Name=default-for-az,Values=true" \
  --query "Subnets[].[SubnetId,AvailabilityZone,CidrBlock]" \
  --output table 2>&1
echo ""

echo "🗄️ REDSHIFT CLUSTERS:"
echo "--------------------"
aws redshift describe-clusters \
  --query "Clusters[].[ClusterIdentifier,ClusterStatus,Endpoint.Address,NodeType,NumberOfNodes]" \
  --output table 2>&1
echo ""

echo "🧠 BEDROCK MODELS:"
echo "-----------------"
echo "Available Bedrock models (first 10):"
aws bedrock list-foundation-models \
  --query "modelSummaries[:10].[modelId,modelName]" \
  --output table 2>&1
echo ""

echo "📁 GITHUB REPO INFO:"
echo "-------------------"
if [ -d ".git" ]; then
    echo "Repository URL: $(git remote get-url origin 2>&1)"
    echo "Current Branch: $(git branch --show-current 2>&1)"
    echo "Last Commit: $(git log -1 --pretty=format:'%h - %s (%an, %ar)' 2>&1)"
    echo "Git Status:"
    git status --porcelain 2>&1
    echo ""
    echo "Remote branches:"
    git branch -r 2>&1
else
    echo "Not in a git repository"
fi
echo ""

echo "📦 LOCAL PYTHON PACKAGES:"
echo "------------------------"
echo "Key packages:"
pip list 2>/dev/null | grep -E "(streamlit|boto3|langchain|sqlalchemy|python-dotenv)" || \
pip3 list 2>/dev/null | grep -E "(streamlit|boto3|langchain|sqlalchemy|python-dotenv)" || \
echo "Could not retrieve pip list"
echo ""

echo "🔧 DEVELOPMENT ENVIRONMENT:"
echo "--------------------------"
echo "Virtual Environment: ${VIRTUAL_ENV:-'Not in virtual environment'}"
echo "Node.js Version: $(node --version 2>/dev/null || echo 'Not installed')"
echo "NPM Version: $(npm --version 2>/dev/null || echo 'Not installed')"
echo "Git Version: $(git --version 2>&1)"
echo ""

echo "💻 EDITOR/IDE CHECK:"
echo "-------------------"
echo "VS Code: $(code --version 2>/dev/null | head -1 || echo 'Not found')"
echo "AWS CLI Extensions:"
aws configure list-profiles 2>&1 | head -10
echo ""

echo "🎯 DEPLOYMENT TARGET:"
echo "--------------------"
echo "Target Region: us-east-1"
echo "Repository: amazon-bedrock-amazon-redshift-poc"
echo "Application Type: Streamlit with Bedrock integration"
echo ""

} | tee "$OUTPUT_FILE"

echo ""
echo "✅ Information gathering complete!"
echo "📄 Results saved to: $OUTPUT_FILE"
echo ""
echo "📋 Next steps:"
echo "1. Share this file with your colleague"
echo "2. Also share the EC2 instance details from the deployment_info_report.json"
echo "3. This will provide a complete picture for creating the deployment guide"