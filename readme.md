A simple database agent that reads your database schema and executes queries based on the prompts you send.
It also saves context between runs

## Installation

We use poetry to manage our dependencies.

```bash
poetry install
```

## Run

First, create a .env file at the project root:

```
# if you're running the database from the compose file, this database url will work
DATABASE_URL=postgresql://user:password@localhost:5432/database
OPENAI_API_KEY=your_openai_api_key
```

Then, run the following command to start the program:

````
```bash
poetry run python main.py
````
