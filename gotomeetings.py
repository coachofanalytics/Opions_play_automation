import json
import os
from datetime import datetime
import requests
import psycopg2

try:
	SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
	SOME_SECRET = "Token not available!"

user_name= os.environ['USER']
password= os.environ['PASSWORD']
host= os.environ['HOST']
port='5432'
database_name = os.environ['DATABASE']


DB_CONFIG = {
	'dbname': database_name,
	'user': user_name,
	'password': password,
	'host': host,
	'port': port
}

def get_db_connection():
	return psycopg2.connect(**DB_CONFIG)

def fetch_meeting_data():
	try:
		conn = get_db_connection()
		cursor = conn.cursor()

		# Select all data from the table
		cursor.execute("SELECT * FROM management_gotomeetings")
		data = cursor.fetchall()

		cursor.close()
		conn.close()
		return data
	except Exception as e:
		print(f"Error fetching data from DB: {e}")
		return None
		
def getmeetingresponse(startDate , endDate):
	access_token = None
	startDateTime="{}T00:00:00Z".format(startDate)
	endDateTime="{}T23:59:00Z".format(endDate)
	print('-'*50)
	dir_path = str(os.getcwd())
	print(dir_path)
	# print("1. getting access tokens")
	# with open(dir_path+'/getdata/gotomeeting/refresh_tokens.json','r') as f:
	# 	myJson = json.load(f)
	# 	access_token = myJson.get('access_token', None)
	# 	if not access_token:
	# 		# Handle the error: log it, raise an exception, or return an error response.
	# 		print("Error: Missing access_token in refresh_tokens.json")
	# 		# return redirect('main:hendler500')
	access_token = 'eyJraWQiOiI2MjAiLCJhbGciOiJSUzUxMiJ9.eyJzYyI6InJlYWQgdHJ1c3Qgd3JpdGUiLCJzdWIiOiIxMDUzODE4NTI1NDg1NDY0NTkwIiwiYXVkIjoiMDA3ZDNhOGUtNWI4ZS00YWY2LThkYjEtYjhkNTI4NTM0MzlmIiwib2duIjoicHdkIiwibHMiOiJhMWJhOGYyMC0xNjQ1LTRkZmQtYmFkYy02Y2YwOWVmYjljMTAiLCJ0eXAiOiJhIiwiZXhwIjoxNjk1MDM4NDcxLCJpYXQiOjE2OTUwMzY2NzEsImp0aSI6IjM1ZTU1OTlmLWE0MGMtNDk0Ny1iOWE4LTRmOGMwY2UzMTU0NCJ9.d2mNui7t1bUUGHrXjjin7_NhWA3qUPkaM_TuORU3rmNH94K9PtuAJmWMXsq4OqMFsNKe39ElJBhucLl6XVpWI1-2AOpAokMq7-6C-Fi6qzOA97ILeoX87PE9v3fCcEYkJWaw1HVAiiUTo8KSw1pvwbgt5LjSnYqd3saVbJkUILf5RdHtJXo1mEarTxGrV6auzsDjQ61jLvcTyxQZHRmchJojSPcfzJQgpV4IRTQ2cldkqRIxNrVKYUVCkLZpPncY1QMsivupUOtoaSawOVHXzwyZyBtzDhfa1Wyuy-1gvaljkHG5n8T7nX0oXoVw05tSO-ZoWog4nRcuV0TbmqUMsQ'
	response = None
	headers = {
	'Authorization': 'Bearer '+access_token
	}
	urlGotoMeeting = "https://api.getgo.com/G2M/rest/historicalMeetings?startDate={}&endDate={}"
	# print("2. getting meetings from {} to {}\n".format(startDate , endDate))
	print(startDate,endDate)
	urlMeeting = urlGotoMeeting.format(startDateTime,endDateTime)
	print("----->urll meeting",urlMeeting)
	response = requests.request("GET" , url=urlMeeting , headers=headers)
	print('-'*50)
	jsonResponse = json.loads(response.text)
	myCleanResponse = []
	attendees1 = []
	for meeting in jsonResponse:
		# print("meetings============>",meeting)
		""" ======== Code For Get attendeeName ========="""
		meeting_id = meeting['meetingId']
		start_time = meeting['startTime']
		urlGotoOneMeetingDetail = f"https://api.getgo.com/G2M/rest/meetings/{meeting_id}/attendees"
		meetingresponse = requests.request("GET" , url=urlGotoOneMeetingDetail , headers=headers)
		attendees_response = json.loads(meetingresponse.text)
		attendee_names = [attendee.get("attendeeName") for attendee in attendees_response]
		
		""" =========== End of Code ============="""
		temp = {}
		meetingItems = meeting.items()
		temp.update(meetingItems)
		if 'recording' in temp.keys():
			temp['recording'] = temp.get('recording').get('shareUrl')
			# print('added rec link')
		else:
			temp['recording'] = "No recording"
		temp['meeting_email'] = temp.get('email')
		temp['startTime'] = temp.get('startTime').replace('T',' ')
		temp['endTime'] = temp.get('endTime').replace('T',' ')

		temp['startTime'] = temp.get('startTime').replace('.+0000','')
		temp['endTime'] = temp.get('endTime').replace('.+0000','')
		temp['attendeeNames'] = [attendee.get('attendeeName') for attendee in attendees_response 
								if attendee.get('startTime') == start_time[:19]] or None
		temp['attendee_Info'] = []
		for attendee in attendees_response:
			if attendee.get('startTime') == start_time[:19]:
				attendee_info = {
					"attendee_duration": attendee.get("duration"),
					"attendee_email": attendee.get("attendeeEmail"),
					"attendee_name": attendee.get("attendeeName")
				}
				temp['attendee_Info'].append(attendee_info)
		
		myCleanResponse.append(temp)
	# return HttpResponse(myCleanResponse)
	return myCleanResponse
	



def save_meeting_data(startDate, endDate):
	# Get the cleaned meeting data
	meeting_data = getmeetingresponse(startDate, endDate)
	print('==============',meeting_data)

	try:
		conn = get_db_connection()
		cursor = conn.cursor()
		current_timestamp = "now()"

		for meeting_info in meeting_data:
			# Extract meeting information
			meeting_topic = meeting_info.get('subject', '')
			meeting_id = meeting_info.get('meetingId', '')
			meeting_type = meeting_info.get('meetingType', '')
			recording = meeting_info.get('recording', '')
			meeting_start_time = meeting_info.get('startTime', '')
			meeting_end_time = meeting_info.get('endTime', '')
			meeting_duration = int(meeting_info.get('duration', '0'))
			meeting_email = meeting_info.get('meeting_email', '')
			attendee_info = meeting_info.get('attendee_Info', [])
			created_at = current_timestamp  # Replace with the appropriate SQL function
			updated_at = current_timestamp  # Replace with the appropriate SQL function
			is_active = True  # Or set it to the desired value
			is_featured = True  # Or set it to the desired value

			# cursor.execute(
			# 	"SELECT COUNT(*) FROM management_gotomeetings WHERE "
			# 	"meeting_id = %s AND meeting_start_time = %s AND meeting_email = %s",
			# 	(meeting_id, meeting_start_time, meeting_email)
			# )
			# existing_meeting_count = cursor.fetchone()[0]
			# if existing_meeting_count == 0:
			for attendee_data in attendee_info:
				attendee_name = attendee_data.get('attendee_name', '')
				attendee_email = attendee_data.get('attendee_email', '')
				attendee_duration = int(attendee_data.get('attendee_duration', '0'))
				
				# SQL query to insert data into the database
				insert_sql = """
					INSERT INTO management_gotomeetings
					(meeting_topic, meeting_id, meeting_type, recording, meeting_start_time, 
					meeting_end_time, meeting_duration, meeting_email, attendee_name, attendee_email, attendee_duration,
					created_at, updated_at,is_active,is_featured)
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
				"""

				cursor.execute(
					insert_sql,
					(meeting_topic, meeting_id, meeting_type, recording, meeting_start_time,
					meeting_end_time, meeting_duration, meeting_email, attendee_name, attendee_email, attendee_duration,
					created_at, updated_at,is_active,is_featured)
				)

		conn.commit()
		print("Meeting data saved to the database.")
	except Exception as e:
		print(f"Error saving meeting data to DB: {e}")
	finally:
		cursor.close()
		conn.close()


# if __name__ == "__main__":
# 	start_date = "2023-09-04"  # Replace with your desired start date
# 	end_date = "2023-09-04"    # Replace with your desired end date

# 	# Call the functions to fetch and save meeting data
# 	fetch_meeting_data()
# 	save_meeting_data(start_date, end_date)
