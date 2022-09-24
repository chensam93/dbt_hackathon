# Git clone repo
$ cd ~/Develop
$ git clone https://github.com/Wind-River-Systems/dbt_hackathon.git

# Create pyenv virtualenv and activate
$ cd ~/Develop/dbt_hackathon
$ pyenv install 3.8.13
$ pyenv local 3.8.13
$ pyenv virtualenv dbt_hackathon
$ pyenv activate dbt_hackathon
# Deactivate virtualenv when conplete
$ pyenv deactivate
$ source deactivate dbt_hackathon

# To remove/delete virtualenv
$ pyenv uninstall dbt_hackathon

# Install Libraries
$ pip install --upgrade pip
# Jupyter Notebooks
$ pip install jupyterlab
# Snoflake Snowpark: https://docs.snowflake.com/en/developer-guide/snowpark/python/setup.html
$ pip install snowflake-snowpark-python
$ pip install "snowflake-snowpark-python[pandas]"
$ pip install "snowflake-snowpark-python[loguru]"

# Create requirements.txt file
$ pip freeze > requirements.txt


#########################
# Serverless - NOT NEEDED, Unless we want to create a Lambda function

# Serverless Reference: https://medium.com/analytics-vidhya/serverless-framework-package-your-lambda-functions-easily-6c4f0351cdab
$ npm install -g serverless

$ serverless create \
 --template aws-python3 \
 --name fivetran-getstream-sync \
 --path fivetran-getstream-sync

# Create package.json
$ npm init

# Install serverless-python-requirements pacakge
$ npm install --save serverless-python-requirements

# Add plugins and custom sections to serverless.yml

# Start Docker

# Deploy serverless to dev
$ serverless deploy --stage dev

# Deploy serverless to prod
$ serverless deploy --stage prod
