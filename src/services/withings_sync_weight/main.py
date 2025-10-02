#!/usr/bin/env python3
"""
Script simple pour transférer le poids de Withings vers Garmin
"""

import os
import subprocess
from dotenv import load_dotenv

# Charge les credentials depuis .env
load_dotenv()

garmin_user = os.getenv('GARMIN_USERNAME')
garmin_pass = os.getenv('GARMIN_PASSWORD')

if not garmin_user or not garmin_pass:
    print("❌ Ajoute GARMIN_USERNAME et GARMIN_PASSWORD dans ton .env")
    exit(1)

# Lance la synchronisation (7 derniers jours par défaut)
print("🔄 Synchronisation Withings → Garmin...")

subprocess.run([
    'withings-sync',
    '--garmin-username', garmin_user,
    '--garmin-password', garmin_pass,
    '-v'
])

print("✅ Terminé!")