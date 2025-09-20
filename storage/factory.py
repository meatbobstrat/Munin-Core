from .sqlite_backend import SQLiteBackend

# from .postgres_backend import PostgresBackend  # (future)


def get_storage_backend(db_type="sqlite", **kwargs):
    if db_type == "sqlite":
        return SQLiteBackend(**kwargs)
    # elif db_type == "postgres":
    #     return PostgresBackend(**kwargs)
    else:
        raise ValueError(f"Unsupported DB type: {db_type}")
