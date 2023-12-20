import os
import re
import json
import pyap
import phonenumbers
import mysql.connector
from bs4 import BeautifulSoup
from email_validator import validate_email, EmailNotValidError

conn = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    password = 'password',
    database = 'wowdb'
)

roletitles =[
    "founder", "director", "president", "vp", "chief", "manager", "lead", "leader", "recruiter", "officer",
    "consultant", "supervisor", "executive", "representative", "specialist", "coordinator", "analyst",
    "accountant", "developer", "administrator", "engineer", "scientist", "designer", "technician",
    "assistant", "partner", "chairman", "head of", "team support", "counsel", "c.{1,2}o"
]

stopwords = [
    "lead", "manage", "operation", "recruit", "contact", "execut", "employ", "search", "work", "services", "industry",
    "team", "about", "compan", "board", "staff", "training", "program", "job", "represent", "story", "mission", "notice",
    "hiring", "techno", "portal", "resource", "support", "engine", "news", "solution", "resume", "help"
]


def get_name_by_splitting_with(char, name, role):
    temp_arr = role.split(char) 

    temp_name = temp_arr[0]
    temp_role = temp_arr[1]

    name_has_role = any(role in temp_name.lower() for role in roletitles)
    name_has_stopword = any(stopword in temp_name.lower() for stopword in stopwords)

    if not name_has_role and not name_has_stopword and not re.search(r'[^a-zA-Z\s.\'’,-]', temp_name):
        return temp_name, temp_role
    else:
        return name, role



def get_person(role_element, roletitles):
    person_role = role_element.get_text()
    person_name = role_element.find_previous().get_text() if role_element.find_previous() else ''
    parent_node = role_element.parent
    parent_text = parent_node.get_text()


    if (not person_name or len(person_name) > 30 or ' ' not in person_name) and re.search(r'(-|–|,|\|)', person_role):
        person_name, person_role = get_name_by_splitting_with((re.search(r'(-|–|,|\|)', person_role)).group(), person_name, person_role)


    if not person_name or len(person_name) > 30 or ' ' not in person_name:
        while parent_text == person_role:
            parent_node = parent_node.parent
            try:
                parent_text = parent_node.get_text()
            except:
                pass
        person_name = parent_text.replace(person_role, '')


    person = {'role': re.sub(r'\s+', ' ', person_role).strip(),
              'name': re.sub(r'\s+', ' ', person_name).strip()}

    return person



def get_people(soup, titles):
    uniquenames = set()
    possibletitles = []
    people = []

    for title in titles:
        possibletitles = soup.find_all(lambda tag: tag.name != 'form' and
                                                   re.search(rf'\b{title}\b', tag.get_text(strip=True, separator=' ').lower()) and
                                                   len(tag.get_text()) < 60)

        for t in possibletitles:
            person = get_person(t, titles)

            name_has_role = any(role in person['name'].lower() for role in titles)
            name_has_stopword = any(stopword in person['name'].lower() for stopword in stopwords)

            if (person['role'] and
                    len(person['name']) < 30 and
                    not re.search(r'[^a-zA-Z\s.\'’,-]', person['name']) and
                    not name_has_role and
                    not name_has_stopword and
                    ' ' in person['name'] and
                    person['name'] not in uniquenames):
                people.append(person)
                uniquenames.add(person['name'])

    return people
#================================================= To get email ids =========================================================

def get_emails(soup):
    emails = set() 
    text_content = soup.get_text().split()
    
    for text in text_content:
        try:
            data = validate_email(text)
            emails.add(data.email)
        except EmailNotValidError:
            pass
    
    
    #to find mailto links
    mailto_links = soup.find_all('a', href=re.compile(r'^mailto:'))
    emails_from_links = [re.search(r'^mailto:(.*?)(?:\?|$)', link['href']).group(1) for link in mailto_links]

    for email in emails_from_links:
        emails.add(email) 
    
    return emails

#===================================================== Get Addresses ========================================================

def get_addresses(soup):
    addresses = set()
    try:
        text_content = soup.body.get_text()
        addresses.update([str(address) for address in pyap.parse(text_content, country="GB")])
        addresses.update([str(address) for address in pyap.parse(text_content, country="CA" )])
        addresses.update([str(address) for address in pyap.parse(text_content, country="US" )])
    except:
        None
    finally:
        return addresses
    
#====================================================== Get Phones ==========================================================

def get_phonenumbers(soup):
    
    phones = set()
    
    try:
        text_content = soup.body.get_text()
        phones = set([match.raw_string for match in phonenumbers.PhoneNumberMatcher(text_content, None)]) 
    except:
        None
    finally:
        return phones
    
#==================================================== To get meta tags ======================================================

def get_metacontent(soup, name):
        tag = soup.find("meta", {'name': name})
        return tag['content'] if tag and tag['content'] else ''

def getmetadata(soup):
    
    metalanguage = soup.html.get('lang', '') if soup.html else ''
    metadescription = get_metacontent(soup, 'description') 
    metakeywords = get_metacontent(soup, 'keywords')

    return {
        "language" : metalanguage, 
        "metakeywords" : metakeywords, 
        "metadescription" : metadescription,
        "isenglish" : True if "en" in metalanguage else False
        #discuss with suraj. html lang may not exist for some sites. doesn't mean the site is non english
        }

#==================================================== Get social links ======================================================

def get_sociallinks(soup):
    linkedin_url = [a['href'] for a in soup.find_all('a', href=lambda href: href and 'linkedin' in href)]
    facebook_url = [a['href'] for a in soup.find_all('a', href=lambda href: href and 'facebook' in href)]
    instagram_url = [a['href'] for a in soup.find_all('a', href=lambda href: href and 'instagram' in href)]
    twitter_url = [a['href'] for a in soup.find_all('a', href=lambda href: href and 'twitter' in href)]

    return {
        "linkedin": " | ".join(linkedin_url) if linkedin_url else '',
        "facebook": " | ".join(facebook_url) if facebook_url else '',
        "instagram": " | ".join(instagram_url) if instagram_url else '',
        "twitter": " | ".join(twitter_url) if twitter_url else ''
        }

#===================================================== is for present =======================================================

def is_form_present(soup):
    forms_with_textarea = soup.select('form textarea')
    
    if(len(forms_with_textarea)>0):
        return True
    else:
        return False
    
    
#=================================================== Execution functions ====================================================

def main(domainpath, cursor):
    people = []
    phones = set()
    addresses = set()
    emails = set()
    
    files = os.listdir(domainpath)
    
    for file in files: 
        with open(os.path.join(domainpath, file), 'r', encoding='utf-8') as file_content: 
            
            html_data = file_content.read()
            soup = BeautifulSoup(html_data, 'html.parser')

            if re.search(r'homepage', file):
                metadata = getmetadata(soup)
                social_links = get_sociallinks(soup)
                # isformonhp = is_form_present(soup)
                
                
            if re.search(r'contact', file):
                None
                # isformoncp = is_form_present(soup)
                
                
            if re.search(r'about|team', file):
                people = people + get_people(soup, roletitles)
                    

            if re.search(r'about|contact', file):
                emails.update(get_emails(soup))
                phones = phones.update(get_phonenumbers(soup))
                addresses.update(get_addresses(soup))
                
               

    print(domainpath)
    
    insert_query = "INSERT INTO wowdb (domain, addresses, contactnumbers, emailids, people, metakeywords, metadescription, language, isenglish, linkedinurls, facebookurls, instagramurls, twitterurls  ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" 
    data_to_insert = (domainpath, " | ".join(addresses), " | ".join(phones), " | ".join(emails), json.dumps(people), metadata['metakeywords'], metadata['metadescription'], metadata['language'], metadata['isenglish'], social_links['linkedin'], social_links['facebook'], social_links['instagram'], social_links['twitter'] )
    cursor.execute(insert_query, data_to_insert) 
    conn.commit()
                    

def executor(path):
    domains = os.listdir(path)
    cursor = conn.cursor()
    for domain in domains:
        main(os.path.join(path, domain), cursor)
    
    cursor.close()
    conn.close()
 
path = "/home/dell/Documents/Mohammed Backup/offline data"
executor(path)
