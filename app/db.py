from sqlalchemy import create_engine
from flask import current_app, g

# from flask.cli import with_appcontext


def get_db():
    if "db" not in g:
        engine = create_engine(f'sqlite:///{current_app.config["DATABASE"]}')

        g.db = engine.connect()

    return g.db


def close_db(e=None):
    db = g.pop("db", None)

    if db is not None:
        db.close()


def init_app(app):
    app.teardown_appcontext(close_db)
