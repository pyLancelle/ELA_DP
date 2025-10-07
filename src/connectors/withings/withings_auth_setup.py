#!/usr/bin/env python3
"""
Withings OAuth Setup Helper
----------------------------
Interactive script to perform the initial OAuth flow with Withings API.
Run this once to save credentials, then future syncs will use the saved tokens.

Usage:
    python3 -m src.connectors.withings.withings_auth_setup
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from connectors.withings.withings_client import WithingsClient


def main():
    """Interactive OAuth setup for Withings API."""
    print("=" * 70)
    print("Withings OAuth Setup - Initial Authentication")
    print("=" * 70)
    print()

    # Load environment variables
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✅ Loaded environment from {env_path}")
    else:
        print(f"⚠️  No .env file found at {env_path}")

    # Get credentials from environment
    client_id = os.getenv("WITHINGS_CLIENT_ID")
    client_secret = os.getenv("WITHINGS_CLIENT_SECRET")

    if not client_id or not client_secret:
        print()
        print("❌ Error: Withings credentials not found in environment!")
        print()
        print("Please add to your .env file:")
        print("  WITHINGS_CLIENT_ID=your_client_id")
        print("  WITHINGS_CLIENT_SECRET=your_client_secret")
        print()
        print("Get these from: https://developer.withings.com/dashboard/")
        sys.exit(1)

    print()
    print(f"✅ Found Withings credentials (client_id: {client_id[:20]}...)")
    print()

    # Initialize client
    client = WithingsClient(
        client_id=client_id,
        client_secret=client_secret,
    )

    # Get authorization URL
    auth_url = client.get_authorization_url()

    print("📋 Step 1: Authorization")
    print("-" * 70)
    print("Open this URL in your browser:")
    print()
    print(auth_url)
    print()
    print("After authorizing, you'll be redirected to a URL like:")
    print(
        "https://jaroslawhartman.github.io/withings-sync/contrib/withings.html?code=XXXXX&state=..."
    )
    print()

    # Get authorization code from user
    print("-" * 70)
    auth_code = input("📝 Paste the authorization code (the XXXXX part): ").strip()

    if not auth_code:
        print("❌ No code provided. Exiting.")
        sys.exit(1)

    print()
    print("🔄 Exchanging code for access token...")

    try:
        # Save credentials
        client.exchange_code_for_token(auth_code)

        print()
        print("=" * 70)
        print("✅ SUCCESS! Withings OAuth setup complete")
        print("=" * 70)
        print()
        print(f"Credentials saved to: {client.credentials_file}")
        print()
        print("You can now run the Garmin sync with Withings integration:")
        print(
            "  python3 -m src.connectors.garmin.garmin_fetch --data-types weight --days 7"
        )
        print()

    except Exception as e:
        print()
        print(f"❌ Error during OAuth flow: {e}")
        print()
        print("Please try again. If the error persists, check:")
        print("  - Your client_id and client_secret are correct")
        print("  - The callback URL matches your app configuration")
        print("  - You copied the full authorization code")
        sys.exit(1)


if __name__ == "__main__":
    main()
