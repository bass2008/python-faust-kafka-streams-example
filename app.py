import faust
from datetime import timedelta


class Purchase(faust.Record):
    id: str
    product_id: str
    quantity: int
    timestamp: float

class Product(faust.Record):
    id: str
    name: str
    price: float

class Alert(faust.Record):
    total_sales: float
    timestamp: float

app = faust.App(
    "product-alert-app",
    broker="kafka://localhost:9092",
    topic_partitions=1,
)

purchases_topic = app.topic("purchases", value_type=Purchase, partitions=1)
products_topic = app.topic("products", value_type=Product, partitions=1)
alerts_topic = app.topic("alerts", value_type=Alert, partitions=1)

products_table = app.Table("products_table", default=Product, partitions=1)
sales_table = app.Table("sales_table", default=float, partitions=1).tumbling(
    timedelta(minutes=1), expires=timedelta(minutes=1)
)

@app.agent(purchases_topic)
async def process_purchase(purchases):
    async for purchase in purchases:
        product = products_table[purchase.product_id]
        if product:
            sale_amount = purchase.quantity * product.price
            current_total = sales_table["total"].now() or 0
            sales_table["total"] = current_total + sale_amount

            if sales_table["total"].now() > 3000:
                total_sales = sales_table["total"].now()
                alert = Alert(total_sales=total_sales, timestamp=purchase.timestamp)
                await alerts_topic.send(value=alert)


@app.agent(products_topic)
async def process_product(products):
    async for product in products:
        products_table[product.id] = product

@app.agent(alerts_topic)
async def process_alert(alerts):
    async for alert in alerts:
        print(f"Received alert: Total sales {alert.total_sales} at {alert.timestamp}")

app.main()
