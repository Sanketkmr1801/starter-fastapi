import httpx
import json
import asyncio
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import datetime
import re
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv("token.env")

MONGO_PASS = os.environ.get("MONGO_PASS")
print(MONGO_PASS)

# MongoDB setup
mongo_uri = f"mongodb+srv://scriz1801:{MONGO_PASS}@cluster0.rittz23.mongodb.net/?retryWrites=true&w=majority"
mongo_client = AsyncIOMotorClient(mongo_uri)
db = mongo_client["HordesAPI"]  # Replace with your actual database name
player_logs_collection = db["player_logs"]

# HTTP client setup
http_client = httpx.Client(http2=True)

class_code = {'warrior': 0, 'mage': 1, 'archer': 2, 'shaman': 3}
faction_code = {"vg": 0, "bl": 1}

url = 'https://hordes.io/api/pve/getbosskillplayerlogs'
headers = {
    'user-agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
}


def formatTime(dt):
  dt = dt.split("T")[0]
  date_obj = datetime.datetime.strptime(dt, "%Y-%m-%d")

  # Format the datetime object
  day = date_obj.strftime("%d")  # day as 0-31
  month = date_obj.strftime("%b")  # month as full month name
  year = date_obj.strftime("%y")  # year as full four-digit number
  dt = f"{day} {month} {year}"

  return dt

def format_seconds(sec):
  minutes = sec // 60
  seconds = sec % 60
  res = ""
  if (minutes == 0):
    res = f"0:{seconds}"
  else:
    res = f"{minutes}:{seconds:02d}"
  return res

def get_player_logs(killID):
    data = {'killid': killID, "sort": 'dps'}
    response = http_client.post(url, headers=headers, data=json.dumps(data))
    player_logs = response.json()

    rows = []
    for player_log in player_logs:
        row = player_log
        row["class_name"] = player_log["class"]
        del row["class"]
        del row["playerid"]
        del row["bossid"]
        del row["patch"]
        del row["ohps"]
        del row["ehps"]
        del row["active"]
        del row["perfdps"]
        del row["perfhps"]
        del row["perfmit"]
        del row["dmg"]
        del row["heal"]
        del row["miti"]
        del row["casts"]
        del row["skills"]
        del row["stats"]
        del row["equip"]
        del row["dpstotal"]
        del row["hpstotal"]
        del row["mpstotal"]
        del row["level"]
        del row["time"]
        del row["gs"]
        rows.append(row)
    for row in rows:
        print(row["killid"])
        break
    return rows

async def get_last_killid_fetched():
    last_document = await player_logs_collection.find_one(sort=[("killid", -1)])
    return last_document["killid"] if last_document else 0

def download_player_records(last_boss_kilid):
    blank_killid_count = 0
    batch_size = 1
    batch = []

    for i in range(last_boss_kilid + 1, sys.maxsize):
        try:
            rows = get_player_logs(i)
            if len(rows) == 0:
                blank_killid_count += 1
            else:
                blank_killid_count = 0

            if blank_killid_count > 3:
                break

            batch.extend(rows)

            # Insert the batch into MongoDB every 1000 records
            print(len(batch))
            if len(batch) >= batch_size:
                player_logs_collection.insert_many(batch)
                print(f"Inserted data into MongoDB for killid {i}")
                batch = []

            print(f"Fetched data for killid {i}")
        except Exception as e:
            print(f"Error downloading player logs for killid {i}: {str(e)}")
            break

    # Insert any remaining records in the batch
    if batch:
        player_logs_collection.insert_many(batch)
        print("Inserted remaining data into MongoDB.")

async def update_db():
    print("updating db now....")
    last_boss_killid = await get_last_killid_fetched()
    print("Last kill id: ", last_boss_killid)
    download_player_records(last_boss_killid)

async def schedule_update_db():
    await update_db()
    while True:
        await asyncio.sleep(2 * 60 * 60)  # Sleep for 2 hours
        await update_db()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to the database when the FastAPI app starts
    print("Starting server...")

    # Schedule update_db every 2 hours
    asyncio.create_task(schedule_update_db())
    await player_logs_collection.create_index("name")

    yield

    # Disconnect from the database when the FastAPI app shuts down
    print("Stopping server...")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/update")
async def update():
    await update_db()
    return {"message": "manually updating db..."}

@app.get("/info/{player_name}")
async def get_player_info(player_name: str):
    pipeline = [
        {
            "$match": {
                "name": {"$regex": f"^{re.escape(player_name.lower())}$", "$options": "i"}
            }
        },
        {
            "$group": {
                "_id": None,
                "kills": {"$sum": 1},
                "deaths": {"$sum": "$deaths"},
            }
        },
    ]

    cursor = player_logs_collection.aggregate(pipeline)

    # Convert the cursor to a list
    result = await cursor.to_list(length=None)
    # Display the result
    print(result)

    if result:
        return {
            "kills": result[0]["kills"],
            "deaths": result[0]["deaths"],
        }
    else:
        return {"error": "Player not found"}
# FastAPI endpoint
@app.get("/rankings/{player_name}/{required_arg}/{optional_args}")
async def ranking(
    player_name: str, required_arg: str, optional_args: str
):
    if player_name == "":
        return {"message": "no player name given"}

    # Update query_params based on options
    optional_params = {
        "vg": "faction",
        "bl": "faction",
        "month": "time",
        "warrior": "class",
        "mage": "class",
        "shaman": "class",
        "archer": "class",
        "true": "duration",
    }

    options = optional_args.split(" ")
    query_params = {"faction": "all", "time": "all", "class": "all", "duration": "false"}

    for option in options:
        option = option.lower()
        print(option)
        if option in optional_params:
            query_params[optional_params[option]] = option


    # Check if the required argument is a valid stat parameter
    valid_stat_params = [
        "dps", "mps", "hps", "min", "max", "crit", "haste", "hp", "mp", "block",
        "defense", "kills", "deaths"
    ]

    if required_arg not in valid_stat_params:
        return {"message": "invalid required_arg"}
    # Log the value of required_arg
    print(f"Logging required_arg: {required_arg}")
    print(query_params)
    sortBy = ""
    if required_arg == "kills":
        sortBy = "kills"
    elif required_arg == "deaths":
        sortBy = "deaths"
    else:
        sortBy = "max"
    print("Sorting by: ", sortBy)
    # Define the aggregation pipeline stages
    pipeline = [
        {
            "$match": {
                "faction": faction_code[query_params["faction"]] if query_params["faction"] != "all" else {"$exists": True},
                "class_name": class_code[query_params["class"]] if query_params["class"] != "all" else {"$exists": True},
                "duration": {"$gt": 60} if query_params["duration"] == "true" else {"$exists": True},
            }
        },
        {
            "$group": {
                "_id": "$name",
                "name": {"$first": "$name"},
                "faction": {"$first": "$faction"},
                "max": {
                    "$max": f"${required_arg}"
                },
                "deaths": {"$sum": "$deaths"},
                "kills": {"$sum": 1},
                "killid": {"$first": "$killid"},
                "time": {"$first": "$time"},
                "duration": {"$first": "$duration"},
            }
        },

        {
            "$sort": {sortBy: -1}
        }
    ]

    # Execute the aggregation pipeline
    cursor = player_logs_collection.aggregate(pipeline)

    results = await cursor.to_list(length=None)

    # print(results)
    result_objects = []

    limit = 10  # Set your desired limit
    for i, row in enumerate(results):
        duration = format_seconds(row['duration'])
        id = row['killid']

        if required_arg in ["crit", "haste", "block"]:
            row['max'] = row['max'] / 10

        record_dict = {
            "index": i + 1,
            "name": row['_id'],
            "duration": duration,
            "id": id,
            "faction": row['faction'],
        }

        if(required_arg == "kills"):
            record_dict["max"] = row["kills"]
        elif(required_arg == "deaths"):
            record_dict["max"] = row["deaths"]
        else:
            record_dict["max"] = row["max"]
            
        if row['_id'].lower() == player_name.lower():
            personal_record = dict(record_dict)
            result_objects.append(personal_record)
            if i > limit:
                break
            continue

        if i < limit:
            result_objects.append(record_dict)

    return {"data": result_objects}

if __name__ == "__main__":
    # asyncio.run(schedule_update_db()) 
    pass
