from zenml import pipeline

from steps.etl import crawl_links, get_or_create_user


@pipeline # Extract, Transform, and Load (ETL) pipeline
def digital_data_etl(user_full_name: str, links: list[str]) -> str:
    user = get_or_create_user(user_full_name) # Get/create user from/in database (Step 1)
    last_step = crawl_links(user=user, links=links) # Crawl links (Step 2)

    return last_step.invocation_id
