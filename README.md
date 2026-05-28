# 🥪 The Jaffle Shop 🦘

_powered by the dbt Fusion engine_

Welcome! This is a sandbox project for exploring the basic functionality of Fusion. It's based on a fictional restaurant called the Jaffle Shop that serves [jaffles](https://en.wikipedia.org/wiki/Pie_iron).

To get started:
1. openssl enc -d -aes-256-cbc -in data.json.enc -out googleworkshopvscode.json 
2. Set up your database connection in `~/.dbt/profiles.yml`. by modifying profiles_example.yml or copying into your profiles.yml file
3. dbtf debug to see if connection worked
4. use either mcp example json to configure MCP in IDE's. Generate token in dbt Platform as a PAT token. 

> [!NOTE]
> If you're brand-new to dbt, we recommend starting with the [dbt Learn](https://learn.getdbt.com/) platform. It's a free, interactive way to learn dbt, and it's a great way to get started if you're new to the tool.

Resources:
https://github.com/dbt-labs/dbt-mcp
https://github.com/dbt-labs/dbt-agent-skills

Google OAuth

`gcloud auth application-default login`
