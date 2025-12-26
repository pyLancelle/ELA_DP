#!/bin/bash
# ELA Data Platform - Infrastructure Setup Script
# This script initializes the GCP infrastructure for Terraform

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
PROJECT_ID="polar-scene-465223-f7"
REGION="europe-west1"
TF_STATE_BUCKET="${PROJECT_ID}-terraform-state"
ARTIFACT_REGISTRY_REPO="ela-dataplatform"

echo -e "${BLUE}=========================================="
echo "ELA Data Platform - Infrastructure Setup"
echo -e "==========================================${NC}"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI is not installed${NC}"
    echo "Please install it: brew install google-cloud-sdk"
    exit 1
fi

# Check if terraform is installed
if ! command -v terraform &> /dev/null; then
    echo -e "${RED}Error: Terraform is not installed${NC}"
    echo "Please install it: brew install terraform"
    exit 1
fi

echo -e "${GREEN}✓ Prerequisites check passed${NC}"
echo ""

# Set GCP project
echo -e "${BLUE}Setting GCP project...${NC}"
gcloud config set project $PROJECT_ID
echo -e "${GREEN}✓ Project set to: $PROJECT_ID${NC}"
echo ""

# Enable required APIs
echo -e "${BLUE}Enabling required GCP APIs...${NC}"
APIS=(
    "run.googleapis.com"
    "cloudbuild.googleapis.com"
    "artifactregistry.googleapis.com"
    "bigquery.googleapis.com"
    "storage.googleapis.com"
    "secretmanager.googleapis.com"
    "iam.googleapis.com"
    "cloudresourcemanager.googleapis.com"
)

for api in "${APIS[@]}"; do
    echo -e "${YELLOW}Enabling $api...${NC}"
    gcloud services enable $api --quiet
done

echo -e "${GREEN}✓ All APIs enabled${NC}"
echo ""

# Create Terraform state bucket
echo -e "${BLUE}Creating Terraform state bucket...${NC}"
if gsutil ls -b gs://$TF_STATE_BUCKET &> /dev/null; then
    echo -e "${YELLOW}Bucket $TF_STATE_BUCKET already exists${NC}"
else
    gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION gs://$TF_STATE_BUCKET
    gsutil versioning set on gs://$TF_STATE_BUCKET
    echo -e "${GREEN}✓ Terraform state bucket created: gs://$TF_STATE_BUCKET${NC}"
fi
echo ""

# Create service account for Terraform (optional, for CI/CD)
SA_NAME="terraform-admin"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo -e "${BLUE}Creating Terraform service account...${NC}"
if gcloud iam service-accounts describe $SA_EMAIL &> /dev/null; then
    echo -e "${YELLOW}Service account $SA_EMAIL already exists${NC}"
else
    gcloud iam service-accounts create $SA_NAME \
        --display-name="Terraform Admin Service Account" \
        --description="Service account for Terraform infrastructure management"

    # Grant necessary roles
    ROLES=(
        "roles/editor"
        "roles/iam.serviceAccountAdmin"
        "roles/resourcemanager.projectIamAdmin"
    )

    for role in "${ROLES[@]}"; do
        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:${SA_EMAIL}" \
            --role="$role" \
            --quiet
    done

    echo -e "${GREEN}✓ Terraform service account created: $SA_EMAIL${NC}"
fi
echo ""

# Summary
echo -e "${GREEN}=========================================="
echo "✓ Setup Complete!"
echo -e "==========================================${NC}"
echo ""
echo "Next steps:"
echo "1. Navigate to infra/terraform/shared and run:"
echo "   cd terraform/shared"
echo "   terraform init"
echo "   terraform plan"
echo "   terraform apply"
echo ""
echo "2. Then deploy the dev environment:"
echo "   cd ../environments/dev"
echo "   terraform init"
echo "   terraform plan"
echo "   terraform apply"
echo ""
echo "3. Finally deploy the prd environment:"
echo "   cd ../prd"
echo "   terraform init"
echo "   terraform plan"
echo "   terraform apply"
echo ""
echo -e "${BLUE}Resources created:${NC}"
echo "  - Terraform state bucket: gs://$TF_STATE_BUCKET"
echo "  - Service account: $SA_EMAIL"
echo ""
echo -e "${YELLOW}Note: You are currently using Application Default Credentials${NC}"
echo -e "${YELLOW}For CI/CD, download a key for the service account:${NC}"
echo "  gcloud iam service-accounts keys create terraform-key.json \\"
echo "    --iam-account=$SA_EMAIL"
echo ""
