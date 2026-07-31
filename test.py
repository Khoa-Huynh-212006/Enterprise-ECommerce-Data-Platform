import random

order_id_rd = random.randint(1, 50)

payment_type_rd = random.choices(
    population=["Momo", "ZaloPay", "Tiền mặt"],
    weights=[60, 30, 10]
)

second_food_rd = random.sample(
    population=["Cafe", "Trà đào", "Bạc xỉu", "Nước suối"],
    k = 2
)

