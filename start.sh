#!/bin/bash

# Install ML packages
python3 python_models/ml-model-downloader.py

# Start the application
pm2-runtime ecosystem.config.js 