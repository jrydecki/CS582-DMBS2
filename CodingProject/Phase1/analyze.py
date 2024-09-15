import json

with open("out.json", "r") as f:
    data = f.read()

json_dict = json.loads(data)

THING = {}

for time, child in json_dict.items():
    last_21_sum = 0
    sma_sum = 0
    rr_sum = 0
    insert_sum = 0

    for x in child["last_21"]:
        last_21_sum += int(x)
    for x in child["sma"]:
        sma_sum += int(x)
    for x in child["rr"]:
        rr_sum += int(x)
    for x in child["insert"]:
        insert_sum += int(x)

    THING[time] = {
        "last_21_sum": round(last_21_sum / 1000000000, 5),
        "sma_sum": round(sma_sum / 1000000000, 5),
        "rr_sum": round(rr_sum / 1000000000, 5),
        "insert_sum": round(insert_sum / 1000000000, 5),
    }

with open("times.json", "w") as f:
    f.write(json.dumps(THING, indent=4))

for time, child in THING.items():
    print(f"Time: {time}")
    print(f"    last_21_sum = {child['last_21_sum']}")
    print(f"    sma_sum = {child['sma_sum']}")
    print(f"    rr_sum = {child['rr_sum']}")
    print(f"    insert_sum = {child['insert_sum']}")