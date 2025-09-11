#!/bin/bash

# Mixpanel Import ETL/LTE Cloud Run Deployment Script
# This script builds and deploys the application to Google Cloud Run
#
# Usage:
#   ./deploy.sh                                    # Use defaults
#   PROJECT_ID=my-project ./deploy.sh              # Override specific values
#   source .env && ./deploy.sh                     # Load from .env file
#
# Environment Variables:
#   PROJECT_ID              Google Cloud Project ID
#   SERVICE_NAME            Cloud Run service name
#   REGION                  Cloud Run region
#   MEMORY                  Memory allocation (e.g., 8Gi)
#   CPU                     CPU allocation (e.g., 4)
#   TIMEOUT                 Request timeout in seconds
#   MAX_INSTANCES           Maximum number of instances
#   MIN_INSTANCES           Minimum number of instances
#   ALLOW_UNAUTHENTICATED   "true" for public access, "false" for authenticated

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration (can be overridden by environment variables)
PROJECT_ID=${PROJECT_ID:-"mixpanel-gtm-training"}
SERVICE_NAME=${SERVICE_NAME:-"etl-lte"}
REGION=${REGION:-"us-central1"}

# Cloud Run specific configuration
MEMORY=${MEMORY:-"8Gi"}
CPU=${CPU:-"4"}
TIMEOUT=${TIMEOUT:-"3600"}
MAX_INSTANCES=${MAX_INSTANCES:-"10"}
MIN_INSTANCES=${MIN_INSTANCES:-"0"}
ALLOW_UNAUTHENTICATED=${ALLOW_UNAUTHENTICATED:-"false"}

echo -e "${BLUE}🚀 Deploying Mixpanel Import ETL/LTE to Cloud Run${NC}"
echo -e "${BLUE}================================================${NC}"
echo -e "Project ID: ${GREEN}${PROJECT_ID}${NC}"
echo -e "Service Name: ${GREEN}${SERVICE_NAME}${NC}"
echo -e "Region: ${GREEN}${REGION}${NC}"
echo -e "Memory: ${GREEN}${MEMORY}${NC}"
echo -e "CPU: ${GREEN}${CPU}${NC}"
echo -e "Timeout: ${GREEN}${TIMEOUT}s${NC}"
echo -e "Instances: ${GREEN}${MIN_INSTANCES}-${MAX_INSTANCES}${NC}"
echo -e "Public Access: ${GREEN}$([ "$ALLOW_UNAUTHENTICATED" = "true" ] && echo "Enabled" || echo "Disabled")${NC}"
echo ""

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}❌ Error: gcloud CLI is not installed${NC}"
    echo -e "${YELLOW}Please install gcloud CLI: https://cloud.google.com/sdk/docs/install${NC}"
    exit 1
fi

# Check if user is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 &> /dev/null; then
    echo -e "${RED}❌ Error: Not authenticated with gcloud${NC}"
    echo -e "${YELLOW}Please run: gcloud auth login${NC}"
    exit 1
fi

# Set the active project
echo -e "${BLUE}🔧 Setting active project...${NC}"
gcloud config set project "${PROJECT_ID}"

# Enable required APIs
echo -e "${BLUE}🔧 Enabling required APIs...${NC}"
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com

# Check if cloudbuild.yaml exists
if [ ! -f "cloudbuild.yaml" ]; then
    echo -e "${RED}❌ Error: cloudbuild.yaml not found in current directory${NC}"
    exit 1
fi

# Check if Dockerfile exists
if [ ! -f "Dockerfile" ]; then
    echo -e "${RED}❌ Error: Dockerfile not found in current directory${NC}"
    exit 1
fi

# Submit build to Cloud Build
echo -e "${BLUE}🏗️  Starting Cloud Build...${NC}"
echo -e "${YELLOW}This may take several minutes...${NC}"

# Build substitution variables for Cloud Build
SUBSTITUTIONS="_PROJECT_ID=${PROJECT_ID}"
SUBSTITUTIONS="${SUBSTITUTIONS},_SERVICE_NAME=${SERVICE_NAME}"
SUBSTITUTIONS="${SUBSTITUTIONS},_REGION=${REGION}"
SUBSTITUTIONS="${SUBSTITUTIONS},_MEMORY=${MEMORY}"
SUBSTITUTIONS="${SUBSTITUTIONS},_CPU=${CPU}"
SUBSTITUTIONS="${SUBSTITUTIONS},_TIMEOUT=${TIMEOUT}"
SUBSTITUTIONS="${SUBSTITUTIONS},_MAX_INSTANCES=${MAX_INSTANCES}"
SUBSTITUTIONS="${SUBSTITUTIONS},_MIN_INSTANCES=${MIN_INSTANCES}"
SUBSTITUTIONS="${SUBSTITUTIONS},_ALLOW_UNAUTHENTICATED=$([ "$ALLOW_UNAUTHENTICATED" = "true" ] && echo "--allow-unauthenticated" || echo "--no-allow-unauthenticated")"

gcloud builds submit \
  --config cloudbuild.yaml \
  --region "${REGION}" \
  --substitutions "${SUBSTITUTIONS}"

# Check if deployment was successful
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Deployment successful!${NC}"
    echo ""
    
    # Get the service URL
    SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
        --region="${REGION}" \
        --format="value(status.url)")
    
    echo -e "${GREEN}🌐 Service URL: ${SERVICE_URL}${NC}"
    echo -e "${GREEN}📊 Health Check: ${SERVICE_URL}/health${NC}"
    echo ""
    echo -e "${BLUE}📋 Useful commands:${NC}"
    echo -e "  View logs: ${YELLOW}gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=${SERVICE_NAME}' --limit 50${NC}"
    echo -e "  View service: ${YELLOW}gcloud run services describe ${SERVICE_NAME} --region=${REGION}${NC}"
    echo -e "  Delete service: ${YELLOW}gcloud run services delete ${SERVICE_NAME} --region=${REGION}${NC}"
    echo ""
else
    echo -e "${RED}❌ Deployment failed!${NC}"
    echo -e "${YELLOW}Check the Cloud Build logs for more details.${NC}"
    exit 1
fi