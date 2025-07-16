#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${GREEN}🐳 DockerHub Deployment Script for airbyte-source${NC}"
echo "Project: airbyte-source"
echo "DockerHub Repository: davidhu314/airbyte-source"
echo ""

# Function to check if docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo -e "${RED}❌ Docker is not running. Please start Docker and try again.${NC}"
        exit 1
    fi
}

# Function to check if logged into DockerHub
check_dockerhub_login() {
    if ! docker info 2>/dev/null | grep -q "Username"; then
        echo -e "${YELLOW}⚠️  You don't appear to be logged into DockerHub.${NC}"
        echo "Would you like to log in now? (y/n)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            docker login
        else
            echo -e "${RED}❌ DockerHub login required to push images.${NC}"
            exit 1
        fi
    fi
}

# Main deployment function
deploy() {
    echo -e "${GREEN}🔨 Building Docker image...${NC}"
    cd "$PROJECT_DIR"
    make build-dockerhub

    echo ""
    echo -e "${GREEN}📤 Pushing to DockerHub...${NC}"
    make push-dockerhub

    echo ""
    echo -e "${GREEN}✅ Deployment complete!${NC}"
    echo "Your image is available at: https://hub.docker.com/r/davidhu314/airbyte-source"
}

# Parse command line arguments
case "${1:-}" in
    "build")
        echo -e "${GREEN}🔨 Building Docker image only...${NC}"
        check_docker
        cd "$PROJECT_DIR"
        make build-dockerhub
        ;;
    "push")
        echo -e "${GREEN}📤 Pushing to DockerHub (will build if needed)...${NC}"
        check_docker
        check_dockerhub_login
        cd "$PROJECT_DIR"
        make push-dockerhub
        ;;
    "login")
        echo -e "${GREEN}🔑 Logging into DockerHub...${NC}"
        docker login
        ;;
    "deploy"|"")
        check_docker
        check_dockerhub_login
        deploy
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  deploy    Build and push to DockerHub (default)"
        echo "  build     Build Docker image only"
        echo "  push      Push to DockerHub (builds if needed)"
        echo "  login     Login to DockerHub"
        echo "  help      Show this help message"
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        echo "Use '$0 help' for usage information."
        exit 1
        ;;
esac 