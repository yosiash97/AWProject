import mysql.connector
import config.py
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd=config.DB_Password,
    database=config.DB
)

mycursor = mydb.cursor()
#mycursor.execute("CREATE TABLE link_watch (city VARCHAR(255), link VARCHAR(255))")
# mycursor.execute("CREATE TABLE rfp_watch (link VARCHAR(255), dates VARCHAR(255))")
# mycursor.execute("CREATE TABLE link_watch (city VARCHAR(255), link VARCHAR(255))")
# mydb.commit()
#mycursor.execute("ALTER TABLE rfp_watch ADD COLUMN keycount INTEGER")

def run_RFP():
    mycursor.execute("DROP TABLE rfp_watch")
    mycursor.execute("CREATE TABLE rfp_watch (link VARCHAR(255), dates VARCHAR(255), keycount VARCHAR(255))")
    mycursor.execute("ALTER TABLE  rfp_watch ADD UNIQUE INDEX(link , keycount)")
    #mycursor.execute("DELETE FROM rfp_watch WHERE link=%s",('www.co.merced.ca.us/bids.aspx',))
    mydb.commit()


def run_links():
    mycursor.execute("DROP TABLE link_watch")
    mycursor.execute("CREATE TABLE link_watch (city VARCHAR(255), link VARCHAR(255))")
    mycursor.execute("ALTER TABLE link_watch ADD UNIQUE INDEX(city)")
    mydb.commit()

def test_RFP():
    mycursor.execute("""UPDATE rfp_watch SET keycount=%s, dates=%s WHERE link=%s""",
                     ("Service", "10/29/2020", 'www.ssf.net/services/rfps-and-bids',))

    mycursor.execute("""UPDATE rfp_watch SET keycount=%s, dates=%s WHERE link=%s""",
                     ("Hello", "10/29/2020", 'cityoforinda.org/Bids.aspx',))
    #mycursor.execute("UPDATE rfp_watch (link, dates, keycount) VALUES (%s, %s, %s)", ('www.cityofsanmateo.org/Bids.aspx', '10/21/2020', "Service"))

run_RFP()
#test_RFP()
run_links()



#mycursor.execute("CREATE TABLE rfp_watch (link VARCHAR(255), dates VARCHAR(255))")
#myresult = mycursor.fetchall()
#mycursor.execute("CREATE TABLE link_watch (city VARCHAR(255), link VARCHAR(255))")
#mycursor.execute("CREATE DATABASE ActiveDB")

# mycursor.execute("SHOW TABLES")

#ALTER USER 'root'@'localhost' IDENTIFIED BY ''; to change password
#print(mycursor.column_names)

