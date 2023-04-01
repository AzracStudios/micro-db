from database import MicroDB
import json


def init(db):
    db.create_table("brands", ["id", "name", "img"], "name")
    db.create_table(
        "products",
        ["id", "name", "price", "img", "qty", "description", "brand"], "name")

    db.create_table("users", ["id", "username", "password", "cart", "orders"],
                    "username")

    # ADD BRANDS
    db.add_to_table("brands", {
        "name": "Zara",
        "img": "/static/assets/zara_logo.png"
    })

    db.add_to_table("brands", {
        "name": "Chanel",
        "img": "/static/assets/chanel_logo.png"
    })

    db.add_to_table("brands", {
        "name": "Dior",
        "img": "/static/assets/dior_logo.png"
    })

    db.add_to_table("brands", {
        "name": "Prada",
        "img": "/static/assets/prada_logo.png"
    })

    # ADD PRODUCTS
    db.add_to_table(
        "products", {
            "name": "Grained Shiny Calfskin & Gold-Tone Metal Lilac",
            "price": "5000",
            "img": "/static/assets/products/image 6.png",
            "qty": "100",
            "description": "Dimensions: 6.4 × 6.8 × 3.9 in ( cm )",
            "brand": "Chanel"
        })

    db.add_to_table(
        "products", {
            "name":
            "Cotton, Calfskin & Silver-Tone Metal Black & White - Large Shopping Bag",
            "price": "4600",
            "img": "/static/assets/products/image 8.png",
            "qty": "100",
            "description": "Dimensions: 11.7 × 19.5 × 8.6 in ( cm )",
            "brand": "Chanel"
        })

    db.add_to_table(
        "products", {
            "name": "Felt Texture Coat",
            "price": "60.8",
            "img": "/static/assets/products/Image.png",
            "qty": "100",
            "description":
            "Open coat with a lapel collar and long sleeves. One of the best-selling products in the world, this innovative coat by Zara is made of 100% felt, which has a wonderful texture. It is a timeless, versatile piece that is perfect for all seasons.",
            "brand": "Zara"
        })

    db.add_to_table(
        "products", {
            "name": "Faux Leather Jacket",
            "price": "48.8",
            "img": "/static/assets/products/Image-3.png",
            "qty": "100",
            "description":
            "Casual jacket with quilted trim and waterfall neckline",
            "brand": "Zara"
        })

    db.add_to_table(
        "products", {
            "name": "TRF Denim Dress",
            "price": "51.8",
            "img": "/static/assets/products/Image-2.png",
            "qty": "100",
            "description":
            "Denim dress with a shirt like design and front pockets",
            "brand": "Zara"
        })

    db.add_to_table(
        "products", {
            "name": "Z1975 Flare Fit Jeans",
            "price": "48.8",
            "img": "/static/assets/products/Image-1.png",
            "qty": "100",
            "description":
            "Faded high-rise jeans featuring pockets on the front and rear patch pockets. Flared hems.",
            "brand": "Zara"
        })


db = MicroDB("my_store")

brands = db.fetch_all_from_table("brands")

print(json.dumps(brands, indent=2))