#!/bin/bash

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  🧪 Testing Docker Image Locally       ║${NC}"
echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo ""

# Cleanup previous test outputs
echo -e "${YELLOW}🧹 Cleaning previous test outputs...${NC}"
rm -rf output/
mkdir -p output/
echo -e "${GREEN}✅ Cleanup done${NC}"
echo ""

# Build image
echo -e "${BLUE}📦 Building Docker image...${NC}"
docker build -t fetcher:test . > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✅ Build successful${NC}"
else
  echo -e "${RED}❌ Build failed${NC}"
  exit 1
fi

# Check image size
IMAGE_SIZE=$(docker images fetcher:test --format "{{.Size}}")
echo -e "   Image size: ${GREEN}${IMAGE_SIZE}${NC}"
echo ""

# Load secrets
echo -e "${BLUE}🔐 Loading secrets from GCP...${NC}"
export SPOTIFY_CLIENT_ID=$(gcloud secrets versions access latest --secret="SPOTIFY_CLIENT_ID" 2>/dev/null)
export SPOTIFY_CLIENT_SECRET=$(gcloud secrets versions access latest --secret="SPOTIFY_CLIENT_SECRET" 2>/dev/null)
export SPOTIFY_REFRESH_TOKEN=$(gcloud secrets versions access latest --secret="SPOTIFY_REFRESH_TOKEN" 2>/dev/null)
export GARMIN_USERNAME=$(gcloud secrets versions access latest --secret="GARMIN_USERNAME" 2>/dev/null)
export GARMIN_PASSWORD=$(gcloud secrets versions access latest --secret="GARMIN_PASSWORD" 2>/dev/null)

if [ -z "$SPOTIFY_CLIENT_ID" ]; then
  echo -e "${RED}❌ Failed to load secrets from GCP${NC}"
  echo -e "${YELLOW}   Make sure you're authenticated: gcloud auth login${NC}"
  exit 1
fi

echo -e "${GREEN}✅ Secrets loaded${NC}"
echo ""

# Test 1: Spotify recently played
echo -e "${BLUE}🎵 Test 1: Spotify recently played (5 tracks)${NC}"
docker run --rm \
  -v $(pwd)/output:/output \
  -e MODE=fetch \
  -e SERVICE=spotify \
  -e SCOPE=recently_played \
  -e DESTINATION=/output \
  -e LIMIT=5 \
  -e SPOTIFY_CLIENT_ID \
  -e SPOTIFY_CLIENT_SECRET \
  -e SPOTIFY_REFRESH_TOKEN \
  fetcher:test > /dev/null 2>&1

if [ $? -eq 0 ]; then
  FILE_COUNT=$(ls output/spotify_recently_played_*.jsonl 2>/dev/null | wc -l)
  if [ $FILE_COUNT -gt 0 ]; then
    LINE_COUNT=$(cat output/spotify_recently_played_*.jsonl | wc -l)
    echo -e "${GREEN}✅ Spotify fetch successful${NC}"
    echo -e "   Generated: ${GREEN}${FILE_COUNT} file(s), ${LINE_COUNT} records${NC}"
  else
    echo -e "${RED}❌ No output files generated${NC}"
    exit 1
  fi
else
  echo -e "${RED}❌ Spotify fetch failed${NC}"
  exit 1
fi
echo ""

# Test 2: Garmin sleep
echo -e "${BLUE}⌚ Test 2: Garmin sleep (3 days)${NC}"
docker run --rm \
  -v $(pwd)/output:/output \
  -e MODE=fetch \
  -e SERVICE=garmin \
  -e SCOPE=sleep \
  -e DESTINATION=/output \
  -e DAYS_HISTORY=3 \
  -e GARMIN_USERNAME \
  -e GARMIN_PASSWORD \
  fetcher:test > /dev/null 2>&1

if [ $? -eq 0 ]; then
  FILE_COUNT=$(ls output/garmin_sleep_*.jsonl 2>/dev/null | wc -l)
  if [ $FILE_COUNT -gt 0 ]; then
    LINE_COUNT=$(cat output/garmin_sleep_*.jsonl | wc -l)
    echo -e "${GREEN}✅ Garmin fetch successful${NC}"
    echo -e "   Generated: ${GREEN}${FILE_COUNT} file(s), ${LINE_COUNT} records${NC}"
  else
    echo -e "${RED}❌ No output files generated${NC}"
    exit 1
  fi
else
  echo -e "${RED}❌ Garmin fetch failed${NC}"
  exit 1
fi
echo ""

# Test 3: Check data quality
echo -e "${BLUE}🔍 Test 3: Data quality checks${NC}"

# Check JSON validity
echo -n "   Validating JSON format... "
cat output/*.jsonl | jq empty > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✅${NC}"
else
  echo -e "${RED}❌ Invalid JSON${NC}"
  exit 1
fi

# Check file sizes
echo -n "   Checking file sizes... "
TOTAL_SIZE=$(du -sh output/ | cut -f1)
echo -e "${GREEN}${TOTAL_SIZE}${NC}"

echo ""

# Summary
echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  ✅ All tests passed!                  ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}📊 Summary:${NC}"
echo -e "   • Docker image: ${GREEN}${IMAGE_SIZE}${NC}"
echo -e "   • Spotify test: ${GREEN}✅ Pass${NC}"
echo -e "   • Garmin test: ${GREEN}✅ Pass${NC}"
echo -e "   • Data quality: ${GREEN}✅ Pass${NC}"
echo -e "   • Total output: ${GREEN}${TOTAL_SIZE}${NC}"
echo ""
echo -e "${GREEN}🚀 Ready to commit and push!${NC}"
echo ""

# Cleanup
read -p "Clean up test outputs? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  rm -rf output/
  docker rmi fetcher:test > /dev/null 2>&1
  echo -e "${GREEN}✅ Cleaned up${NC}"
fi
