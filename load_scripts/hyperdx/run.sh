#!/bin/bash
set -e

# Step 1: Run the updater script with the downloaded archive
python updater.py source.tar.gz

# Step 2: Copy sample.tar.gz to the bucket
gsutil cp sample.tar.gz gs://hyperdx/

# Step 3: Make the uploaded file publicly readable
gsutil acl ch -u AllUsers:R gs://hyperdx/sample.tar.gz