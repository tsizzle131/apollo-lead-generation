#!/bin/bash
# Reliable runner script for lead generation pipeline
# This ensures we always use the correct Python environment

cd "/Users/tristanwaite/n8n test/lead_generation"
/Users/tristanwaite/miniconda3/bin/python main.py "$@"