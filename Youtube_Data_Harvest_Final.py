import os
import sys
import subprocess

# Auto-install required packages
required_packages = [
    "streamlit",
    "sqlalchemy",
    "pymysql",
    "mysql-connector-python",
    "pandas",
    "streamlit-option-menu",
    "google-api-python-client"
]

# Function to install required packages
def install_packages():
    print("Checking and installing required packages...")
    for package in required_packages:
        try:
            __import__(package)
            print(f"{package} is already installed.")
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"{package} has been installed.")

# Install packages if needed
if __name__ == "__main__":
    install_packages()

# Now import the required packages
import googleapiclient.discovery
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import datetime
import pymysql

#API key Connection:
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyBTlX0rpwoLvCqmmqDHQyKQFx_VuE_iIzo"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# Getting Channel Details:
def channel_data(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()
    for i in response.get('items', []):
        data = {
            "channel_Id": channel_id,
            "channel_name": i['snippet']['title'],
            "channel_dec": i['snippet']['description'],
            "Playlist_Id": i['contentDetails']['relatedPlaylists']['uploads'],
            "Video_count": i['statistics']['videoCount'],
            "sub_count": i['statistics']['subscriberCount'],
            "view_count": i['statistics']['viewCount'],
        }
        return data

# Getting Video Details:
def Get_Video_Ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part="contentDetails").execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    NextPageToken = None

    while True:
        response_A = youtube.playlistItems().list(part='snippet', maxResults=50, playlistId=playlist_id, pageToken=NextPageToken).execute()
        for i in range(len(response_A['items'])):
            video_ids.append(response_A['items'][i]['snippet']['resourceId']['videoId'])
        NextPageToken = response_A.get('nextPageToken')
        if NextPageToken is None:
            break
    return video_ids, playlist_id

# Getting video information function:
def get_video_info(video_ids, playlistId, channel_name):
    video_data = []
    for video_info in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_info)
        response = request.execute()

        # To change duration time from ISO to Seconds
        def time_duration(t):    
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        for video in response["items"]:
            # Convert ISO datetime format to MySQL format
            published_date = datetime.datetime.strptime(
                video['snippet']['publishedAt'], 
                '%Y-%m-%dT%H:%M:%SZ'
            ).strftime('%Y-%m-%d %H:%M:%S')
            
            data = {
                "video_Id": video['id'],
                "Playlist_Id": playlistId,
                "video_name": video['snippet']['title'],
                "channel_name": channel_name,
                "video_Description": video['snippet'].get('description', ''),
                "Published_Date": published_date,  # Use the formatted date
                "View_count": int(video['statistics'].get('viewCount', 0)),
                "Like_count": int(video['statistics'].get('likeCount', 0)),
                "Comments_count": int(video['statistics'].get('commentCount', 0)),
                "Favorite_Count": int(video['statistics'].get('favoriteCount', 0)),
                "Duration": time_duration(video['contentDetails']['duration']),
                "Thumbnail": video['snippet']['thumbnails']['default']['url'],
                "Caption_Status": video['contentDetails']['caption']
            }
            video_data.append(data)
    return video_data

# Get comment information function
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            try:
                request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=50)
                response = request.execute()
                for i in response['items']:
                    # Convert ISO datetime format to MySQL format
                    comment_published_at = datetime.datetime.strptime(
                        i['snippet']['topLevelComment']['snippet']['publishedAt'], 
                        '%Y-%m-%dT%H:%M:%SZ'
                    ).strftime('%Y-%m-%d %H:%M:%S')
                    
                    data = {
                        "Comment_Id": i['snippet']['topLevelComment']['id'],
                        "video_Id": i['snippet']['topLevelComment']['snippet']['videoId'],
                        "Comment_text": i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "comment_author": i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_Published_at": comment_published_at  # Use the formatted date
                    }
                    Comment_data.append(data)
            except Exception as e:
                print(f"Error fetching comments for video {video_id}: {e}")
                continue
    except Exception as e:
        print(f"Error in comment collection process: {e}")
    return Comment_data

# Final data function
def finaldata(channel_id):
    Channel_Details = channel_data(channel_id)
    Video_Ids, playlist_id = Get_Video_Ids(channel_id)
    Video_Details = get_video_info(Video_Ids, playlist_id, Channel_Details['channel_name'])  # Pass channel_name here
    Comment_information = get_comment_info(Video_Ids)

    youtube_data = {
        "channel": Channel_Details,
        "videoid": Video_Ids,
        "video": Video_Details,
        "comment": Comment_information
    }
    return youtube_data

# Database connection function
def connect_to_database():
    try:
        # Database password
        db_password = "6382582755jK*"  # Using the password from your mysql.connector connection
        
        # Create MySQL connection
        mydb = mysql.connector.connect(
            host="localhost", 
            user="root", 
            password=db_password
        )
        mycursor = mydb.cursor(buffered=True)
        
        # Create database if it doesn't exist
        mycursor.execute('CREATE DATABASE IF NOT EXISTS you_7')
        mycursor.execute('USE you_7')
        
        # Create SQLAlchemy engine with proper password - URL encoded for special characters
        password_encoded = db_password.replace("*", "%2A")  # URL encode the asterisk
        engine = create_engine(f"mysql+pymysql://root:{db_password}@localhost/you_7")
        
        # Create tables
        mycursor.execute("""CREATE TABLE IF NOT EXISTS channel (
                    channel_Id VARCHAR(255) PRIMARY KEY,
                    channel_name VARCHAR(255),
                    channel_dec TEXT,
                    Playlist_Id  VARCHAR(255),
                    Video_count  INT,
                    sub_count  INT,  
                    view_count INT)""")

        mycursor.execute("""CREATE TABLE IF NOT EXISTS video(
                        video_Id VARCHAR(255) PRIMARY KEY,
                        Playlist_Id  VARCHAR(255),
                        video_name VARCHAR(255),
                        channel_name VARCHAR(255),
                        video_Description TEXT, 
                        Published_Date DATETIME,
                        View_count INT,
                        Like_count INT,
                        Comments_count INT,
                        Favorite_Count INT,
                        Duration TIME,
                        Thumbnail VARCHAR(255),
                        Caption_Status VARCHAR(255)  
                        )""")

        mycursor.execute("""CREATE TABLE IF NOT EXISTS comment(
                Comment_Id VARCHAR(255),
                Video_Id VARCHAR(255),
                FOREIGN KEY(Video_Id) REFERENCES video(Video_Id),
                Comment_text TEXT,
                comment_author VARCHAR(255),
                Comment_Published_at DATETIME
                        )""")
                        
        return mydb, mycursor, engine
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None, None, None

# Main Streamlit app
def main():
    st.set_page_config(page_title='YouTube Data Harvesting and Warehousing',
                   layout='wide',
                   initial_sidebar_state='expanded',
                   menu_items={'About': '''This streamlit application was developed by M.Aravinth.
                                Contact_e_mail: aravinth7m@gmail.com'''})
    
    # Connect to database
    mydb, mycursor, engine = connect_to_database()
    
    # Display error if database connection failed
    if not mydb or not engine:
        st.error("Failed to connect to database. Please check your MySQL installation and credentials.")
        st.info("Make sure MySQL is running and that you have the correct password.")
        return

    # Sidebar menu
    with st.sidebar:
        selected = option_menu("Main Menu",
                           ["Home", "Data collection", "MYSQL Database", "Analysis using SQL", "Data Visualization"],
                           icons=["house", "cloud-upload", "database", "filetype-sql", "bar-chart-line"],
                           menu_icon="menu-up",
                           orientation="vertical")

    if selected == "Home":
        st.title(':red[You]Tube :black[Data Harvesting & Warehousing]')
        st.subheader(':blue[Domain :] Social Media')
        st.subheader(':blue[Summary:]')
        st.markdown('''Develop a basic application using Streamlit for 
                    the purpose of retrieving YouTube channel data by utilizing the YouTube API. 
                    The acquired data should be stored in an SQL database managed by the XAMPP control panel, 
                    allowing for querying using SQL. Additionally, visualize the data within the Streamlit application
                    to reveal insights and trends associated with the YouTube channel's data''')
        st.subheader(':red[Skills Take Away :]')
        st.markdown(''':red[Python scripting,Data Collection,API integration,Data Management using SQL,Streamlit]''')
        st.subheader(':green[About :]')
        st.markdown('''Hello Everyone! Jaya Kumar Kathirvel here..🎓 I completed my Bachelor of Engineering (BE) and went on to gain 4 years of valuable experience in the Oil & Gas industry as a Site Coordinator at QCON in Qatar 🛠️. While my journey in the engineering field has been fulfilling, I’ve always been passionate about technology and data.

🔍 Recognizing the growing impact and future potential of Data Science, I’ve shifted my career path towards this exciting domain. I’m currently enhancing my skills and actively seeking opportunities in Data Science, where I can combine my analytical mindset with real-world problem solving 📊💡.

🚀 I believe in continuous learning, adaptability, and bringing data-driven insights to life through powerful tools like Python, SQL, and Streamlit. I’m eager to contribute to innovative teams and grow in this dynamic field! 🌐✨''')
        st.subheader(':blue[Contact:]')
        st.markdown('#### linkedin: https://www.linkedin.com/in/jaya-kumar-kathirvel-586723184')
        st.markdown('#### Email : jayakumark302000@gmail.com')

    if selected == "Data collection":
        st.header('Youtube data Harvesting and Warehousing')
        st.subheader('Welcome !')
        channel_id = st.text_input('**Enter the Channel ID**')
        st.write('(**Collects data** by using :orange[channel id])')
        Get_data = st.button('**Collect Data**')

        if Get_data:
            if not channel_id:
                st.warning("Please enter a valid Channel ID")
            elif not mydb or not engine:
                st.error("Database connection failed. Please check your database configuration.")
            else:
                with st.spinner("Collecting data from YouTube..."):
                    try:
                        finaloutput = finaldata(channel_id)
                        
                        if finaloutput and "channel" in finaloutput:
                            st.success("Data collected from YouTube successfully!")
                            
                            # Handle channel insert - use ON DUPLICATE KEY UPDATE
                            try:
                                channel_data = finaloutput['channel']
                                query = """
                                INSERT INTO channel 
                                (channel_Id, channel_name, channel_dec, Playlist_Id, Video_count, sub_count, view_count) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                channel_name = VALUES(channel_name),
                                channel_dec = VALUES(channel_dec),
                                Playlist_Id = VALUES(Playlist_Id),
                                Video_count = VALUES(Video_count),
                                sub_count = VALUES(sub_count),
                                view_count = VALUES(view_count)
                                """
                                mycursor.execute(query, (
                                    channel_data['channel_Id'],
                                    channel_data['channel_name'],
                                    channel_data['channel_dec'],
                                    channel_data['Playlist_Id'],
                                    channel_data['Video_count'],
                                    channel_data['sub_count'],
                                    channel_data['view_count']
                                ))
                                mydb.commit()
                                st.success("Channel data inserted/updated successfully!")
                            except Exception as e:
                                st.error(f"Error with channel data: {e}")
                            
                            # Handle video inserts - using direct SQL instead of pandas to_sql
                            video_insert_count = 0
                            for video in finaloutput['video']:
                                try:
                                    query = """
                                    INSERT INTO video 
                                    (video_Id, Playlist_Id, video_name, channel_name, video_Description, 
                                    Published_Date, View_count, Like_count, Comments_count, Favorite_Count, 
                                    Duration, Thumbnail, Caption_Status) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE 
                                    Playlist_Id = VALUES(Playlist_Id),
                                    video_name = VALUES(video_name),
                                    channel_name = VALUES(channel_name),
                                    video_Description = VALUES(video_Description),
                                    Published_Date = VALUES(Published_Date),
                                    View_count = VALUES(View_count),
                                    Like_count = VALUES(Like_count),
                                    Comments_count = VALUES(Comments_count),
                                    Favorite_Count = VALUES(Favorite_Count),
                                    Duration = VALUES(Duration),
                                    Thumbnail = VALUES(Thumbnail),
                                    Caption_Status = VALUES(Caption_Status)
                                    """
                                    mycursor.execute(query, (
                                        video['video_Id'],
                                        video['Playlist_Id'],
                                        video['video_name'],
                                        video['channel_name'],
                                        video['video_Description'],
                                        video['Published_Date'],
                                        video['View_count'],
                                        video['Like_count'],
                                        video['Comments_count'],
                                        video['Favorite_Count'],
                                        video['Duration'],
                                        video['Thumbnail'],
                                        video['Caption_Status']
                                    ))
                                    video_insert_count += 1
                                except Exception as e:
                                    print(f"Error with video {video['video_Id']}: {e}")
                            
                            mydb.commit()
                            st.success(f"Video data: {video_insert_count} videos processed")
                            
                            # Handle comment inserts
                            comment_insert_count = 0
                            for comment in finaloutput['comment']:
                                try:
                                    # Check if the video exists first to maintain foreign key integrity
                                    check_query = "SELECT 1 FROM video WHERE video_Id = %s"
                                    mycursor.execute(check_query, (comment['video_Id'],))
                                    if mycursor.fetchone():
                                        query = """
                                        INSERT INTO comment 
                                        (Comment_Id, video_Id, Comment_text, comment_author, Comment_Published_at) 
                                        VALUES (%s, %s, %s, %s, %s)
                                        ON DUPLICATE KEY UPDATE 
                                        Comment_text = VALUES(Comment_text),
                                        comment_author = VALUES(comment_author),
                                        Comment_Published_at = VALUES(Comment_Published_at)
                                        """
                                        mycursor.execute(query, (
                                            comment['Comment_Id'],
                                            comment['video_Id'],
                                            comment['Comment_text'],
                                            comment['comment_author'],
                                            comment['Comment_Published_at']
                                        ))
                                        comment_insert_count += 1
                                except Exception as e:
                                    print(f"Error with comment {comment['Comment_Id']}: {e}")
                            
                            mydb.commit()
                            st.success(f"Comment data: {comment_insert_count} comments processed")
                            
                        else:
                            st.error("Failed to collect data. Please check the Channel ID and try again.")
                    except Exception as e:
                        st.error(f"Error processing data: {e}")

    if selected == "MYSQL Database":
        st.header("MySQL Database Operations")
        st.info("This section allows you to view the data stored in your MySQL database.")
        
        if not mydb or not engine:
            st.error("Database connection failed. Please check your database configuration.")
            return
            
        # Add database viewing functionality here
        table_option = st.selectbox("Select a table to view", ["channel", "video", "comment"])
        
        if st.button("View Data"):
            try:
                query = f"SELECT * FROM {table_option}"
                df = pd.read_sql(query, engine)
                st.write(f"Total rows: {len(df)}")
                st.dataframe(df)
            except Exception as e:
                st.error(f"Error fetching data: {e}")

    if selected == "Analysis using SQL":
        st.header("SQL Analysis")
        st.info("Run SQL queries to analyze your YouTube data")
        
        # Predefined queries
        queries = {
            "1. What are the names of all the videos and their corresponding channels?": 
                "SELECT video_name, channel_name FROM video",
            "2. Which channels have the most number of videos?": 
                "SELECT channel_name, COUNT(*) as video_count FROM video GROUP BY channel_name ORDER BY video_count DESC",
            "3. What are the top 10 most viewed videos?": 
                "SELECT video_name, channel_name, view_count FROM video ORDER BY view_count DESC LIMIT 10",
            "4. How many comments were made on each video?": 
                "SELECT v.video_name, COUNT(c.Comment_Id) as comment_count FROM video v LEFT JOIN comment c ON v.video_Id = c.Video_Id GROUP BY v.video_Id, v.video_name",
            "5. Which videos have the highest number of likes?": 
                "SELECT video_name, channel_name, like_count FROM video ORDER BY like_count DESC LIMIT 10",
            "6. What is the total duration of all videos in each channel?": 
                "SELECT channel_name, SEC_TO_TIME(SUM(TIME_TO_SEC(Duration))) as total_duration FROM video GROUP BY channel_name",
            "7. Which channels have the highest average video duration?": 
                "SELECT channel_name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(Duration))), '%H:%i:%s') as avg_duration FROM video GROUP BY channel_name ORDER BY AVG(TIME_TO_SEC(Duration)) DESC",
            "8. When was the most recent video published?": 
                "SELECT video_name, channel_name, Published_Date FROM video ORDER BY Published_Date DESC LIMIT 1",
            "9. What are the top 5 most popular videos based on likes/views ratio?": 
                "SELECT video_name, channel_name, ROUND((like_count/view_count)*100, 2) as engagement_percent FROM video WHERE view_count > 0 ORDER BY engagement_percent DESC LIMIT 5",
        }
        
        query_selection = st.selectbox("Select an analysis query", list(queries.keys()))
        
        if st.button("Run Query"):
            try:
                query = queries[query_selection]
                df = pd.read_sql(query, engine)
                st.dataframe(df)
            except Exception as e:
                st.error(f"Error executing query: {e}")

    if selected == "Data Visualization":
        st.header("Data Visualization")
        st.info("Visualize the insights from your YouTube data")
        
        # Add visualization code here
        chart_type = st.selectbox("Choose visualization", [
            "Top 5 Channels by Video Count",
            "Top 10 Videos by Views",
            "Top 10 Videos by Likes",
            "Comments Distribution by Video"
        ])
        
        if st.button("Generate Visualization"):
            try:
                if chart_type == "Top 5 Channels by Video Count":
                    query = "SELECT channel_name, COUNT(*) as video_count FROM video GROUP BY channel_name ORDER BY video_count DESC LIMIT 5"
                    df = pd.read_sql(query, engine)
                    st.bar_chart(df.set_index('channel_name'))
                    
                elif chart_type == "Top 10 Videos by Views":
                    query = "SELECT video_name, view_count FROM video ORDER BY view_count DESC LIMIT 10"
                    df = pd.read_sql(query, engine)
                    st.bar_chart(df.set_index('video_name'))
                    
                elif chart_type == "Top 10 Videos by Likes":
                    query = "SELECT video_name, like_count FROM video ORDER BY like_count DESC LIMIT 10"
                    df = pd.read_sql(query, engine)
                    st.bar_chart(df.set_index('video_name'))
                    
                elif chart_type == "Comments Distribution by Video":
                    query = """
                    SELECT v.video_name, COUNT(c.Comment_Id) as comment_count 
                    FROM video v 
                    LEFT JOIN comment c ON v.video_Id = c.Video_Id 
                    GROUP BY v.video_Id, v.video_name
                    ORDER BY comment_count DESC
                    LIMIT 10
                    """
                    df = pd.read_sql(query, engine)
                    st.bar_chart(df.set_index('video_name'))
                    
            except Exception as e:
                st.error(f"Error generating visualization: {e}")

# Run the application
if __name__ == "__main__":
    main()