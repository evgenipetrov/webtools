#!/bin/bash

# Define the subfolder, the string to find, and the replacement string
SUBFOLDER="src"
SEARCH_STRING="auth_domain"
REPLACE_STRING="auth_email"

# Find all files in the subfolder and replace the string
find $SUBFOLDER -type f -exec sed -i 's/'"$SEARCH_STRING"'/'"$REPLACE_STRING"'/g' {} +