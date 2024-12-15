
DATABASE_CONFIG = {
    'host': '10.0.0.241',
    'port': 3310,
    'user': 'root',
    'password': 'root',
    'database': 'db_movies_netflix_transact'
}

DATABASE_CONFIG_2 = {
    'host': '10.0.0.241',
    'port': 3310,
    'user': 'root',
    'password': 'root',
    'database': 'dw_netflix'
}

CSV_FILES = {
    'Awards_movie': 'data/Awards_movie.csv',
    'Awards_participants': 'data/Awards_participants.csv',
    'users': 'data/users.csv'
}

LOG_FILE = 'logs/pipeline.log'

QUERY = """
SELECT 
    movie.movieID as movieID, movie.movieTitle as title, movie.releaseDate as releaseDate, 
    gender.name as gender , person.name as participantName, participant.participantRole as roleparticipant 
    FROM movie 
    INNER JOIN participant 
    ON movie.movieID=participant.movieID
    INNER JOIN person
    ON person.personID = participant.personID
    INNER JOIN movie_gender 
    ON movie.movieID = movie_gender.movieID
    INNER JOIN gender 
    ON movie_gender.genderID = gender.genderID
    """

COLUMNS = {'releaseDate': 'releaseMovie', 'Award': 'awardMovie'}
COL_DROP = ['IdAward']

LOAD_ORDER = ['dimMovie','dimUser',"FactWatchs"]

DATAFRAMES = {}