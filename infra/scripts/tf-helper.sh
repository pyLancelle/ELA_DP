#!/bin/bash
# Terraform Helper Script
# Provides convenient shortcuts for common Terraform operations

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

show_usage() {
    echo "Terraform Helper Script"
    echo ""
    echo "Usage: ./tf-helper.sh <command> [environment]"
    echo ""
    echo "Commands:"
    echo "  init <env>        Initialize Terraform for environment (shared|dev|prd)"
    echo "  plan <env>        Run terraform plan for environment"
    echo "  apply <env>       Run terraform apply for environment"
    echo "  destroy <env>     Run terraform destroy for environment"
    echo "  output <env>      Show outputs for environment"
    echo "  list <env>        List all resources in state"
    echo "  import <env>      Interactive import wizard"
    echo "  fmt               Format all Terraform files"
    echo "  validate <env>    Validate Terraform configuration"
    echo ""
    echo "Environments: shared, dev, prd"
    echo ""
    echo "Examples:"
    echo "  ./tf-helper.sh init dev"
    echo "  ./tf-helper.sh plan dev"
    echo "  ./tf-helper.sh apply dev"
    echo "  ./tf-helper.sh output dev"
}

get_terraform_dir() {
    local env=$1
    if [ "$env" = "shared" ]; then
        echo "terraform/shared"
    else
        echo "terraform/environments/$env"
    fi
}

cmd_init() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${BLUE}Initializing Terraform for $env environment...${NC}"
    cd $tf_dir
    terraform init
    echo -e "${GREEN}✓ Initialization complete${NC}"
}

cmd_plan() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${BLUE}Planning changes for $env environment...${NC}"
    cd $tf_dir
    terraform plan
}

cmd_apply() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${YELLOW}This will apply changes to $env environment${NC}"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi

    echo -e "${BLUE}Applying changes for $env environment...${NC}"
    cd $tf_dir
    terraform apply
    echo -e "${GREEN}✓ Apply complete${NC}"
}

cmd_destroy() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${RED}WARNING: This will DESTROY all resources in $env environment${NC}"
    read -p "Type the environment name to confirm: " confirm
    if [ "$confirm" != "$env" ]; then
        echo "Confirmation failed. Aborted."
        exit 0
    fi

    echo -e "${BLUE}Destroying resources in $env environment...${NC}"
    cd $tf_dir
    terraform destroy
    echo -e "${GREEN}✓ Destroy complete${NC}"
}

cmd_output() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${BLUE}Outputs for $env environment:${NC}"
    cd $tf_dir
    terraform output
}

cmd_list() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${BLUE}Resources in $env environment:${NC}"
    cd $tf_dir
    terraform state list
}

cmd_import() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${BLUE}Import Wizard for $env environment${NC}"
    echo ""
    echo "What would you like to import?"
    echo "1. Cloud Run Job"
    echo "2. BigQuery Dataset"
    echo "3. GCS Bucket"
    echo "4. Secret"
    read -p "Choice (1-4): " choice

    cd $tf_dir

    case $choice in
        1)
            read -p "Job name (e.g., spotify-recently-played): " job_name
            terraform import "module.cloud_run_jobs.google_cloud_run_v2_job.jobs[\"$job_name\"]" \
                "projects/polar-scene-465223-f7/locations/europe-west1/jobs/$job_name"
            ;;
        2)
            read -p "Dataset ID (e.g., raw): " dataset_id
            terraform import "module.bigquery.google_bigquery_dataset.dataset[\"$dataset_id\"]" \
                "projects/polar-scene-465223-f7/datasets/$dataset_id"
            ;;
        3)
            read -p "Bucket name (e.g., ela-dp-dev): " bucket_name
            terraform import module.storage.google_storage_bucket.bucket $bucket_name
            ;;
        4)
            read -p "Secret name (e.g., SPOTIFY_CLIENT_ID): " secret_name
            terraform import "module.secrets.google_secret_manager_secret.secrets[\"$secret_name\"]" \
                "projects/polar-scene-465223-f7/secrets/$secret_name"
            ;;
        *)
            echo "Invalid choice"
            exit 1
            ;;
    esac

    echo -e "${GREEN}✓ Import complete${NC}"
}

cmd_fmt() {
    echo -e "${BLUE}Formatting all Terraform files...${NC}"
    cd terraform
    terraform fmt -recursive
    echo -e "${GREEN}✓ Formatting complete${NC}"
}

cmd_validate() {
    local env=$1
    local tf_dir=$(get_terraform_dir $env)

    echo -e "${BLUE}Validating configuration for $env environment...${NC}"
    cd $tf_dir
    terraform validate
    echo -e "${GREEN}✓ Validation successful${NC}"
}

# Main script
COMMAND=$1
ENV=$2

if [ -z "$COMMAND" ]; then
    show_usage
    exit 1
fi

# Navigate to infra directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

case $COMMAND in
    init|plan|apply|destroy|output|list|import|validate)
        if [ -z "$ENV" ]; then
            echo -e "${RED}Error: Environment required${NC}"
            show_usage
            exit 1
        fi
        if [ "$ENV" != "shared" ] && [ "$ENV" != "dev" ] && [ "$ENV" != "prd" ]; then
            echo -e "${RED}Error: Invalid environment. Must be shared, dev, or prd${NC}"
            exit 1
        fi
        cmd_$COMMAND $ENV
        ;;
    fmt)
        cmd_fmt
        ;;
    *)
        echo -e "${RED}Error: Unknown command '$COMMAND'${NC}"
        show_usage
        exit 1
        ;;
esac
