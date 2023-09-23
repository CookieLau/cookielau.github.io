import requests
import json
import datetime
import logging
import sqlite3

logger = logging.getLogger("pipeline")
logger.setLevel(logging.DEBUG)
# add print time for logger
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

PREFETCH_MONTHS = 4 # query the events for the next 4 months
MAIN_URL_TEMPLATE = "https://nus.edu.sg/cfg/calendar/events/{year}/{month}"
EVENT_URL_TEMPLATE = "https://nus.edu.sg/cfg/events/details/{event_id}"
TABLE_ITEM_TEMPLATE = """
<tr>
  <td><img width="100" height="100"
          src="{logo_url}" />
  </td>
  <td> <a href="{event_url}">{event_title}</a>
  </td>
  <td style="white-space: nowrap;">{start_date}</td>
  <td style="white-space: nowrap;">{end_date}</td>
  <td style="white-space: nowrap;">{registration_ddl}</td>
  <td><a href="{external_link}">🔗</a></td>
  <td>{industry}</td>
  <td>
    {faculty}
  </td>
  <td>{target}</td>
  <td hidden>{description}</td>
</tr>"""
REPLACE_WORD_FOR_TABLE_ITEMS = "REPLACE_ME_HERE_WITH_TABLE_CONTENTS"

# split faculty by common and format with "<p>{faculty}</p>"
def split_and_format_faculties(faculties) -> str:
  return "<p>" + "</p><p>".join(faculties.split(",")) + "</p>"

def get_registration_ddl(item) -> str:
  event_url = EVENT_URL_TEMPLATE.format(event_id=item['EventId'])
  ret = requests.get(event_url)
  registration_ddl = "N/A"
  if ret.status_code != 200:
    logger.error("Failed to get event page for event id: %s", item['EventId'])
  else:
    SPLIT_WORDS = ["<strong>Register by&nbsp;", "<strong>Register by "]
    for word in SPLIT_WORDS:
      if word in ret.text:
        registration_ddl = ret.text.split(word)[1].split("<")[0]
        break
    registration_ddl = registration_ddl.replace("&nbsp;", " ")
    registration_ddl = registration_ddl.replace(".", "")
    if registration_ddl == "N/A" and "Register" not in ret.text:
      logger.warning("No registration ddl found for event id: %s", item['EventId'])
      registration_ddl = "None"
  if registration_ddl == "N/A":
    logger.error("Failed to get registration ddl for event id: %s", item['EventId'])
  elif registration_ddl != "None":
    registration_ddl = datetime.datetime.strptime(registration_ddl, "%d %B %Y").date()
  return registration_ddl

class EventDB(object):

  ITEM_DICT = ["EventId", "EventTitle", "StartDateTimeStr", "EndDateTimeStr", "RegistrationDDL", "ExternalLink", "Industry", "Faculty", "TargetAudience", "Description", "Logo"]

  def __init__(self, db_name):
    self.conn = sqlite3.connect(db_name)
    self.cursor = self.conn.cursor()
    # check if the table exists
    self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
    if self.cursor.fetchone() is None:
      self.create_table()

  def create_table(self):
    self.cursor.execute("""CREATE TABLE IF NOT EXISTS events (
      event_id TEXT PRIMARY KEY,
      event_title TEXT,
      start_date TEXT,
      end_date TEXT,
      registration_ddl TEXT,
      external_link TEXT,
      industry TEXT,
      faculty TEXT,
      target TEXT,
      description TEXT,
      logo TEXT
    )""")

  def insert_event(self, item, registration_ddl):
    logger.debug("Inserting event %s with regis ddl: %s", item['EventId'], registration_ddl)
    self.cursor.execute("""INSERT INTO events VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )""", (
      item['EventId'],
      item['EventTitle'],
      item['StartDateTimeStr'].split("T")[0],
      item['EndDateTimeStr'].split("T")[0],
      registration_ddl,
      item['ExternalLink'],
      item['Industry'],
      split_and_format_faculties(item['Faculty']),
      item['TargetAudience'],
      item['Description'],
      item['Logo']
    ))
    self.conn.commit()

  def contains(self, event_id):
    self.cursor.execute("SELECT * FROM events WHERE event_id=?", (event_id,))
    return self.cursor.fetchone() is not None
  
  def get_all_events(self):
    self.cursor.execute("SELECT * FROM events")
    return self.cursor.fetchall()
  
  def get_non_expired_events(self):
    self.cursor.execute("SELECT * FROM events WHERE end_date >= ?", (datetime.datetime.now().date(),))
    return self.cursor.fetchall()

  def generate_non_expired_table(self):
    events = self.get_non_expired_events()
    table = ""
    for event in events:
      item = dict(zip(self.ITEM_DICT, event))
      table += self.__generate_table_event(item)
    return table

  def __generate_table_event(self, item):
    return TABLE_ITEM_TEMPLATE.format(
      logo_url=item['Logo'], 
      event_url=EVENT_URL_TEMPLATE.format(event_id=item['EventId']),
      event_title=item['EventTitle'],
      start_date=item['StartDateTimeStr'].split("T")[0],
      end_date=item['EndDateTimeStr'].split("T")[0],
      registration_ddl=item['RegistrationDDL'],
      external_link=item['ExternalLink'],
      industry=item['Industry'],
      faculty=split_and_format_faculties(item['Faculty']),
      target=item['TargetAudience'],
      description=item['Description']
    )
  
if __name__ == "__main__":
  today = datetime.date.today()

  db = EventDB("events.db")

  for i in range(PREFETCH_MONTHS):
    month = today.month + i - 1 # -1 because the API is 0-indexed
    year = today.year
    if month > 12:
      month -= 12
      year += 1
    logger.info("Fetching events for %d-%d", year, month)
    ret = requests.get(MAIN_URL_TEMPLATE.format(year=year, month=month))
    if ret.status_code != 200:
      logger.error("Failed to fetch events for %d-%d", year, month)
      continue
    events = json.loads(ret.text)
    for item in events:
      if not db.contains(item['EventId']):
        logger.info("Inserting event %s", item['EventId'])
        registration_ddl = get_registration_ddl(item)
        db.insert_event(item, registration_ddl)
        logger.info("Inserted event %s", item['EventId'])
  
  logger.info("Generating table")
  utcnow = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
  singapore_time = utcnow.astimezone(tz=datetime.timezone(datetime.timedelta(hours=8)))
  table = "<h3>Last updated: {}</h3>".format(singapore_time) + db.generate_non_expired_table()
  logger.info("Table generated")

  with open("template.html", "r") as f:
      html_template = f.read()
    
  new_page = html_template.replace(REPLACE_WORD_FOR_TABLE_ITEMS, table)

  # set a new page with date and time
  new_html_name = "output/output_" + str(singapore_time.date()) + "_" + str(datetime.datetime.now().time()).replace(":", "-") + ".html"
  with open(new_html_name, "w") as f:
      f.write(new_page)
  with open("index.html", "w") as f:
      f.write(new_page)
  logger.info("New page generated: %s", new_html_name)

  