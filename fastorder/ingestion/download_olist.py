from pathlib import Path 
import requests

project_root = Path(__file__).resolve().parents[2]

data_dir = project_root / "data" / "raw" /"olist"

base_url = "https://raw.githubusercontent.com/" \
            "olist/olist-dataset/master/dataset/"

olist_datasets = {
    "customers.csv": "olist_customers_dataset.csv",
    "orders.csv": "olist_orders_dataset.csv",
    "order_items.csv": "olist_order_items_dataset.csv",
    "order_payments.csv": "olist_order_payments_dataset.csv",
    "products.csv": "olist_products_dataset.csv",
    "sellers.csv": "olist_sellers_dataset.csv",
    "order_reviews.csv": "olist_order_reviews_dataset.csv",
    "geolocation.csv": "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv": "olist_product_category_name_translation.csv",
}

def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists():
        print(f"[SKIP] {destination.name} already exists")
        return
    print(f"[DOWNLOAD] {destination.name}")

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with open(destination, "wb") as f:
        f.write(response.content)
    print(f"[DONE] {destination.name} downloaded successfully")

def main() ->None: 
    """
    Download all Olist datasets
    """
    print("=" * 60)
    print("Downloading Olist datasets...")
    print("=" * 60)

    for local_name, remote_name in olist_datasets.items():
        url = f"{base_url}/{remote_name}"
        destination = data_dir / local_name
        download_file(url, destination)

    print("=" * 60)
    print("All Olist datasets downloaded successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
