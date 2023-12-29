import os
import re
import json
import sqlite3
import pyap
import phonenumbers
import pycountry
import mysql.connector
import asyncio
import traceback
import sys
from datetime import datetime
from datasource import urls
from pyppeteer import launch
from email_validator import validate_email, EmailNotValidError
from user_agent import generate_user_agent
from urllib.parse import urlparse


conn = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    password = 'password',
    database = 'wowdb'
)

database_file = 'domainsWithApps.db' 
conn = sqlite3.connect(database_file)


#===================================================== constants ============================================================

create_query = '''
    CREATE TABLE IF NOT EXISTS appdomains ( tld TEXT, name TEXT, crawlstarttime TEXT, crawlendtime TEXT, contacturl TEXT,
    abouturl TEXT, leadershipurl TEXT, careersurl TEXT, facebookurl TEXT, twitterurl TEXT, instagramurl TEXT, linkedinurl TEXT, 
    pinteresturl TEXT, youtubeurl TEXT, hometitle TEXT, homekeywords TEXT, homedescription TEXT, twitterhandle TEXT,
    ucategory TEXT, gmapsurl TEXT, phones TEXT, gphone Text, emails TEXT, addresses TEXT, gaddress Text, pcodes TEXT, people TEXT, 
    isgoodurl INTEGER, ogtitle TEXT, ogdescription TEXT, ogurl TEXT, ogimage TEXT, oglocale TEXT, ogsitename TEXT, 
    language TEXT, isenglish INTEGER, isformonhp INTEGER, isformoncp INTEGER )
    '''

insert_query = '''
    INSERT INTO appdomains ( tld,  name, crawlstarttime, crawlendtime, contacturl, abouturl, leadershipurl, careersurl, 
    facebookurl, twitterurl, instagramurl, linkedinurl, pinteresturl, youtubeurl, hometitle, homekeywords, homedescription, 
    twitterhandle, ucategory, gmapsurl, phones, gphone, emails, addresses, gaddress, pcodes, people, isgoodurl, ogtitle, ogdescription, ogurl, 
    ogimage, oglocale, ogsitename, language, isenglish, isformonhp, isformoncp ) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

regex_name_validator = '''^[A-Z]([a-zA-Z'’,\-]+|\.)(?:\s+[A-Z]([a-zA-Z'’,\-]+|\.{0,1})){0,3}$'''

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


fullTitles = [
    "founder", "director", "president", "vp", "chief", "manager", "lead", "leader", "recruiter", "officer",
    "consultant", "supervisor", "executive", "representative", "specialist", "coordinator", "analyst",
    "accountant", "developer", "administrator", "engineer", "scientist", "designer", "technician",
    "assistant", "partner", "chairman", "head of", "team support", "counsel"
]

acronymTitles = [
    "ceo", "cfo", "coo", "cto", "cmo", "chro", "cio"
]

xpath_fulltitle = '''
//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'title') and 
string-length(normalize-space(text()))<60 
and not(ancestor::form)]
'''

xpath_acronymtitle = '''
 //*[(
  contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'title ') or
  contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'title/') or 
  contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'title,') or
  contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'title-')) and
  string-length(normalize-space(text()))<65 and not(ancestor::form)] |
 //*[translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='title']
   '''
   
   
#==================================================== Norton methods =========================================================

async def get_text_content(page, selector):
    await page.waitFor(selector)
    text_content = await page.evaluate(f'''(selector) => {{
        let element = document.querySelector('{selector}');
        return element ? element.textContent : '';
    }}''', selector)
    
    return text_content


async def get_norton_rating(page):

    rating = 0
    xpath_ratingparent = '//*[name()="g" ]'
    xpath_rating = '//div/svg-icon//*[name()="g" ]//*[name()="rect" and not(@fill="#C1BFB8")]'

    await page.waitForXPath(xpath_ratingparent)
    rating_element = await page.xpath(xpath_rating)

    if (len(rating_element) > 0):
        rating = await page.evaluate('(element) => { return parseInt(element.getAttribute("width")) }''', rating_element[0])

    return round(rating / 20)



async def get_norton_tags(page):

    tags = ''
    selector_tagsHeading = 'div.tags-heading'
    selector_tagscontainer = 'div.tags-content-details'
    selector_viewmore = '//div[@class="tag-list"]//li[@class="view-more-link xsmall-body-text-bold"]'
    selector_tags = '//div[@class="tag-list"]//li'


    await page.waitFor(selector_tagsHeading)
    await page.click(selector_tagsHeading)

    await page.waitFor(selector_tagscontainer)
    view_more_element = await page.xpath(selector_viewmore)

    while (len(view_more_element) > 0):
        await view_more_element[0].click()
        view_more_element = await page.xpath(selector_viewmore)

    li_Elements = await page.xpath(selector_tags)

    if (len(li_Elements) > 0): 
        tags = await asyncio.gather(*((await page.evaluate('(e) => {return e.textContent.trim()}', li)) for li in li_Elements))
    return ' | '.join(tags)



#================================================= Reverse Lookup trial =======================================================

def get_places_on_cp(text_content):
    places = set([])
   
    try:
        for i in pycountry.countries:
            if i.name in text_content:
                print("Countries found") 
                places.add(i.name)

    except:
        print("Failed")

    return places


async def get_text_from_ancestor_of(page, childselector, ancestorselector):
    
    text_content = await page.evaluate(f'''(childselector, ancestorselector) => {{
        let child = document.querySelector('{childselector}');
        let ancestor = child ? child.closest('{ancestorselector}') : '';
        return ancestor ? ancestor.textContent : '';
    }}''', childselector, ancestorselector)

    return text_content

async def get_gmap_data(page, domain, country):
    
    xpath_results = '//div[@class="Nv2PK tH5CWc THOPZb "]'
    xpath_no_results = '//div[text()="Google Maps can\'t find "]'
    url = "https://www.google.com/maps/search/" + domain.split('.')[0] # + "," + country

    await page.goto(url) 

    results = await page.xpath(xpath_results)
    no_results = await page.xpath(xpath_no_results)
    
    if len(no_results) > 0:
        return "not found"

    
    if len(results) == 0:
        website = await page.evaluate('''()=> { 
            let l = document.querySelector("a.lcr4fd.S9kvJb") 
            return l ? l.href : ''
        }''') 
        if domain in website:
            map_data = await get_result_data(page)
            return map_data


    if len(results) > 0:
        for result in results:
            website = await page.evaluate('(container)=> container.querySelector("a.lcr4fd.S9kvJb").href', result)
            website = urlparse(website.replace('https://www.google.com/url?q=', '')).netloc.replace('www.','')
            if domain == website:
                await result.click()
                await page.waitForXPath('//div[@class="RcCsl fVHpi w4vB1d NOE9ve M0S7ae AG25L "]')
                map_data = await get_result_data(page)
                return map_data
            
    return "no results matched"
            
async def get_result_data(page): 
    
    mapsdata = {}
    
    mapsdata["business_name"] = await get_text_content(page, "h1.DUwDvf.lfPIob") 
    mapsdata["business_category"] = await get_text_content(page, "button.DkEaL") 
    mapsdata["business_rating"] = await get_text_content(page, "div.F7nice") 
    mapsdata["business_address"] = await get_text_from_ancestor_of(page, 'img[src="//www.gstatic.com/images/icons/material/system_gm/1x/place_gm_blue_24dp.png"]', "div.AeaXub")
    mapsdata["business_phone"] = await get_text_from_ancestor_of(page, 'img[src="//www.gstatic.com/images/icons/material/system_gm/1x/phone_gm_blue_24dp.png"]', "div.AeaXub")
    mapsdata["business_pcodes"] = await get_text_from_ancestor_of(page, 'img[src="//maps.gstatic.com/mapfiles/maps_lite/images/2x/ic_plus_code.png"]', "div.AeaXub")

    await (await page.xpath('//button[@data-value="Share"]'))[0].click()
    await page.waitForXPath('//input[@class="vrsrZe"]') 
    mapsdata["gmaps_link"] = await page.evaluate('(element) => element.value',(await page.xpath('//input[@class="vrsrZe"]'))[0])

    return mapsdata

#=============================================== Getting People Names ====================================================

async def get_person(page, role_element, roletitles, stopwords):
    
    person = await page.evaluate('''(e, roletitles, stopwords) => {
        
            let personrole = e.textContent;
            let personname = e.previousElementSibling ? e.previousElementSibling.textContent : ''
            let parentnode = e.parentNode;
            let parentText = parentnode.textContent;

            function getNameBySplittingWith(char, name, role) {
            const [tempname, temprole] = role.split(char).filter(el => el)

            const nameHasRole = roletitles.some(role => tempname.toLowerCase().includes(role));
            const nameHasStopword = stopwords.some(stopword => tempname.toLowerCase().includes(stopword));

            if (!nameHasRole && !nameHasStopword && !tempname.match(/[^a-zA-Z\s.'’,-]/))
                return [tempname, temprole]
            else
                return [name, role]

            }
 
 
            //const hasSplitChar = personrole.match(/–|-|\||,/);
            //if (hasSplitChar) {
            //    const char = hasSplitChar[0]
            //    if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes(char)) {
            //        [personname, personrole] = getNameBySplittingWith(char, personname, personrole);
            //    }
            //}


            if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes('–'))
                ([personname, personrole] = getNameBySplittingWith('–', personname, personrole))

            if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes('-'))
                ([personname, personrole] = getNameBySplittingWith('-', personname, personrole));

            if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes('|'))
                ([personname, personrole] = getNameBySplittingWith('|', personname, personrole))

            if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes(','))
                ([personname, personrole] = getNameBySplittingWith(',', personname, personrole));


            if (!personname || personname.length > 30 || !personname.includes(' ')) {

                while (parentText == personrole) {
                    parentnode = parentnode.parentNode
                    parentText = parentnode.textContent
                }

                personname = parentText.replace(personrole, '')
            }

            return { role: personrole.replace(/\s+/g, ' ').trim(), name: personname.replace(/\s+/g, ' ').trim() };
        }
    ''', role_element, roletitles, stopwords)
    return person



async def get_people(page, titles, title_xpath, stopwords):
    uniquenames = set()
    possibletitles = []
    people = []

    for title in titles: 
        possibletitles = await page.xpath(title_xpath.replace('title', title))

        for t in possibletitles:
            person = await get_person(page, t, titles, stopwords)
            # print(person)
            name_has_role = any(role in person['name'].lower() for role in titles)
            name_has_stopword = any(stopword in person['name'].lower() for stopword in stopwords)

            if (
                person['name'] !=''
                and len(person['name']) < 30
                and bool(re.match(r'^[a-zA-Z\s.\'’,-]+$', person['name']))
                and not name_has_role
                and not name_has_stopword
                and ' ' in person['name']
                and person['name'] not in uniquenames
            ):
                people.append(person)
                uniquenames.add(person['name'])

    return people

#================================================= To get email ids =========================================================

async def get_emails(page, text_content):
    emails = set() 
    
    for text in text_content.split():
        try:
            data = validate_email(text)
            emails.add(data.email)
        except EmailNotValidError:
            pass
    
    
    #to find mailto links
    mailto_links = await page.xpath('//a[contains(@href, "mailto:")]')
    if(len(mailto_links)>0):
        emails_from_links = [re.search(r'^mailto:(.*?)(?:\?|$)', (await page.evaluate( '(giveaway) => giveaway.href', link))).group(1) for link in mailto_links]
        for email in emails_from_links:
            emails.add(email) 
    
    return emails

#===================================================== Get Addresses ========================================================

def get_addresses(text_content):
    addresses = set()
    try:
        addresses.update([str(address) for address in pyap.parse(text_content, country="GB")])
        addresses.update([str(address) for address in pyap.parse(text_content, country="CA" )])
        addresses.update([str(address) for address in pyap.parse(text_content, country="US" )])
    except:
        None
    finally:
        return addresses
    
    #use reverselookup for addresses
    
#====================================================== Get Phones ==========================================================

def get_phonenumbers(text_content):
    
    phones = set()
    
    try:
        phones = set([match.raw_string for match in phonenumbers.PhoneNumberMatcher(text_content, None)]) 
    except:
        None
    finally:
        return phones
    
#==================================================== To get meta data ======================================================

async def get_metadata(page):
    
    metadata = await page.evaluate('''() => {
        let metadata = {}
        
        function getmetacontent(attr, value){
            const e = document.querySelector(`meta[${attr}='${value}']`);
            return e ? e.content.replace(/\s+/g, ' ') : ''
        }
        
        function gettextcontent(tag){
            const e = document.querySelector(tag)
            return e ? e.textContent.replace(/\s+/g, ' ') : ''
        }
        
        let langattr = document.querySelector('html').getAttribute('lang')
        metadata.language = langattr ? langattr : ''
        metadata.title = gettextcontent('title')
        metadata.keywords = getmetacontent('name', 'keywords')
        metadata.ogurl = getmetacontent('property', 'og:url')
        metadata.ogtitle = getmetacontent('property', 'og:title')
        metadata.ogimage = getmetacontent('property', 'og:image')
        metadata.oglocale = getmetacontent('property', 'og:locale')
        metadata.description = getmetacontent('name', 'description')
        metadata.ogsitename = getmetacontent('property', 'og:site_name')
        metadata.ogtwitterhandle = getmetacontent('name', 'twitter:site')
        metadata.ogdescription = getmetacontent('property', 'og:description')
        metadata.isenglish = metadata.language.includes('en') ? true : false   
        
            
        return metadata
        }''')
  

    return metadata

#==================================================== Get social links ======================================================
 
async def get_social_links(page):
    
    social_links = await page.evaluate('''() => {
        
        let socialLinks = {}
        
        function isSocialPage(href) {
            
            const parts = href.split('/').filter(part => part);
            const lastPart = parts.length > 0 ? parts.pop() : '';
            return /^[a-zA-Z0-9]+$/.test(lastPart);
            
        }

        function getSocialLinks(platform){
            
            const links = Array.from(document.querySelectorAll(`a[href*="${platform}"]`)).filter(a =>  {
                const href = a.getAttribute('href');
                return href && href.length < 80 && isSocialPage(href);
            });
        
            return Array.from(new Set(links.map(a => a.getAttribute('href')))).join(' | ');
        }

        socialLinks.linkedin = getSocialLinks('linkedin')
        socialLinks.facebook = getSocialLinks('facebook')
        socialLinks.instagram = getSocialLinks('instagram')
        socialLinks.twitter = getSocialLinks('twitter')
        socialLinks.youtube = getSocialLinks('youtube')
        socialLinks.pinterest = getSocialLinks('pinterest')

        return socialLinks
        
    }''')

    return social_links



#================================================= getting url by keyword ====================================================

async def get_useful_pages(page):
 
    useful_pages = await page.evaluate('''() => {
        
        let usefulPages = {}
        const domain = window.location.href
        
        function isValidInternalHref(domain, href) {
            return href && !href.startsWith('mailto:') && (!href.startsWith('http') || (href.startsWith('http') && href.includes(domain.replace(/http:\/\/|httpa:\/\/|www\./g))));
        }

        function containsKeywordInText(text, keywords) { 
            return keywords.some(keyword => text.includes(keyword));
        }
        
        function containsKeywordInLastPart(href, keywords) {
            const parts = href.split('/').filter(part => part);
            const lastPart = parts.length > 0 ? parts.pop() : '';
            return keywords.some(keyword => lastPart.toLowerCase().includes(keyword));
        }

        function getHrefsWithKeywords(keywords){
            const elements = Array.from(document.querySelectorAll('a[href]')).filter(a => {
                const href = a.getAttribute('href')
                const text = a.textContent
                return isValidInternalHref(domain, href) && (containsKeywordInLastPart(href, keywords) || containsKeywordInText(text, keywords));
            });
            
            let formattedUrls = elements.map(a => {
                    href = a.getAttribute('href')
                    if(href.startsWith('http'))
                        return href
                    else
                        return new URL(href, domain).toString()
                })
                
            return [...new Set(formattedUrls)]
        }


        usefulPages.contact = getHrefsWithKeywords(['contact']); 
        usefulPages.leadership= getHrefsWithKeywords(['team', 'leaders']); 
        usefulPages.career = getHrefsWithKeywords(['career']); 
        usefulPages.about = getHrefsWithKeywords(['about']); 
        
        return usefulPages
        
    }''')
    
    return useful_pages


#===================================================== is form present =======================================================

async def is_visible_form_present(page):
    
    is_visible_form_on_page = False
    
    is_visible_form_on_page = await page.evaluate('''(forms)=> {
        
        function isElementVisible(e) {
            
            let elementVisible = false
        
            const fieldheight = e.getBoundingClientRect().height
            const fieldwidth = e.getBoundingClientRect().width
            const ariaHiddenAttribute = e.getAttribute('hidden')
            const hiddenAttribute = e.getAttribute('aria-hidden')
            const disabledAttribute = e.getAttribute('disabled')
            const computedStyle = window.getComputedStyle(e)
            const display = computedStyle.getPropertyValue('display');
            const opacity = parseFloat(computedStyle.getPropertyValue('opacity'));
            const visibility = computedStyle.getPropertyValue('visibility');
    
            if (
                (display && display !== 'none') &&
                (opacity && opacity > 0) &&
                (visibility && visibility !== 'hidden') &&
                (fieldwidth > 0) &&
                !hiddenAttribute &&
                (!ariaHiddenAttribute || ariaHiddenAttribute !== 'true') &&
                !disabledAttribute
            ) {
                elementVisible = true;
            }

            return elementVisible
        }
      
        function hasNoInvisibleAncestor(element){
            let ancestor = element.parentElement
            while (ancestor) {
                const displayStyle = getComputedStyle(ancestor).getPropertyValue('display');
                if (displayStyle === 'none') {
                    return false;
                }
                ancestor = ancestor.parentElement;
            }
            return true
        }      
      
    let textareas = Array.from(document.querySelectorAll('textarea'));
    let formsWithTextareas = Array.from(textareas.map(e => e.closest('form')));

    let visibleForms = formsWithTextareas.filter(isElementVisible)
    
    if(visibleForms.length > 0)
        visibleForms = visibleForms.filter(hasNoInvisibleAncestor)
    
    return visibleForms.length>0 ? true : false
        
    }''')

            
    
    return is_visible_form_on_page

#======================================================= is Url good ========================================================

async def is_url_good(metadata):
    
    bad_keywords = ['domain', 'not found', 'error']
    
    if any(keyword in metadata["title"].lower() for keyword in bad_keywords) or any(keyword in metadata["description"].lower() for keyword in bad_keywords):
        return False

    return True

    
#=================================================== Execution functions ====================================================

def init_domain_data():
    return {
        "crawlstarttime" : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "crawlendtime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "people" : [],
        "isformoncp": False,
        "isformonhp": False,
        "phones" : set([]),
        "addresses" : set([]),
        "emails" : set([]), 
        "isgoodurl" : False,
        "metadata" : { "language":"", "title":"", "keywords":"", "ogurl":"", "ogtitle"  :"", "ogimage":"", "oglocale":"", "description":"", "ogsitename":"", "ogtwitterhandle":"", "ogdescription":"", "isenglish":""},
        "social_links" : {"linkedin":"", "facebook" :"", "instagram" :"", "twitter" :"", "youtube":"", "pinterest":"" },
        "useful_pages": { "contact":"", "leadership":"", "career":"", "about":"" },
        "gmaps_data" : { "business_name":"", "business_category":"", "business_rating":"", "business_address":"", "business_phone":"", "gmaps_link":"", "business_pcodes":""}
    }


async def main(domain, cursor):
    
    domain_data = init_domain_data() 
    
    
    browser = await launch(
        headless = False,
        defaultViewport = {'width': 0, 'height': 0},
        args = [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--fast-start',
        '--disable-extensions',
        '--start-maximized',
        ]
    )
    
    try:
        page = await browser.newPage()
        # nortonurl = 'https://safeweb.norton.com/report?url=arctern.com'
        # selector_result = 'p.rating-label.xl-body-text-bold'
        # selector_category = 'div.category-list'

        # await page.setUserAgent(generate_user_agent(os=('mac', 'linux')))
        # await page.goto(nortonurl)
        
        # domain_data["norton_status"] = await get_text_content(page, selector_result)
        # domain_data["norton_category"] = await get_text_content(page, selector_category)
        # domain_data["norton_rating"] = await get_norton_rating(page)
        # domain_data["norton_tags"] = await get_norton_tags(page)
        
        await page.goto(domain) 
        
        domain_data["metadata"] = await get_metadata(page)
        domain_data["isgoodurl"] = await is_url_good(domain_data["metadata"])
        
        if domain_data["isgoodurl"] is False:
            print('domain is parked')
            return domain_data
        
        domain_data["social_links"] = await get_social_links(page)
        domain_data["useful_pages"] = await get_useful_pages(page)
        domain_data["isformonhp"] = await is_visible_form_present(page)
                
           
        for cp in domain_data["useful_pages"]['contact']:
            
            await page.goto(cp)
            text_content = re.sub('\s+',' ',await page.evaluate('document.body.textContent', force_expr=True)) 
            
            domain_data["isformoncp"] = domain_data["isformoncp"] or (await is_visible_form_present(page))
            domain_data["emails"].update(await get_emails(page, text_content))
            domain_data["phones"].update(get_phonenumbers(text_content))
            domain_data["addresses"].update(get_addresses(text_content))
            
        for tp in domain_data["useful_pages"]['leadership']:
            await page.goto(tp)
            domain_data["people"] = domain_data["people"] +  (await get_people(page, fullTitles, xpath_fulltitle, stopwords))
            domain_data["people"] = domain_data["people"] +  (await get_people(page, acronymTitles, xpath_acronymtitle, stopwords))


        hostname = urlparse(domain).netloc.replace('www.','')
        domain_data["gmaps_data"] = await get_gmap_data(page,  hostname, '')  
        domain_data["crawlendtime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(domain_data)
    
    except Exception as error:
        traceback.print_exc(file=sys.stdout)
    
    finally:
        await browser.close()
        cursor.execute(create_query)



    data_to_insert = (
    domain,
    domain_data["gmaps_data"]["business_name"],
    domain_data["crawlstarttime"],
    domain_data["crawlendtime"],
    " | ".join(domain_data["useful_pages"]['contact']),
    " | ".join(domain_data["useful_pages"]['about']),
    " | ".join(domain_data["useful_pages"]['leadership']),
    " | ".join(domain_data["useful_pages"]['career']),
    domain_data["social_links"]['facebook'], 
    domain_data["social_links"]['twitter'],
    domain_data["social_links"]['instagram'], 
    domain_data["social_links"]['linkedin'], 
    domain_data["social_links"]['pinterest'], 
    domain_data["social_links"]['youtube'], 
    domain_data["metadata"]['title'],
    domain_data["metadata"]['keywords'], 
    domain_data["metadata"]['description'],
    domain_data["metadata"]['ogtwitterhandle'],
    domain_data["gmaps_data"]["business_category"],
    domain_data["gmaps_data"]["gmaps_link"],
    " | ".join(domain_data["phones"]), 
    domain_data["gmaps_data"]["business_phone"],
    " | ".join(domain_data["emails"]), 
    " | ".join(domain_data["addresses"]),
    domain_data["gmaps_data"]["business_address"],
    domain_data["gmaps_data"]["business_pcodes"],
    json.dumps(domain_data["people"]), 
    domain_data["isgoodurl"],
    domain_data["metadata"]['ogtitle'],
    domain_data["metadata"]['ogdescription'],
    domain_data["metadata"]['ogurl'],
    domain_data["metadata"]['ogimage'],
    domain_data["metadata"]['oglocale'],
    domain_data["metadata"]['ogsitename'],
    domain_data["metadata"]['language'], 
    domain_data["metadata"]['isenglish'], 
    domain_data["isformonhp"],
    domain_data["isformoncp"]
)

    cursor.execute(insert_query, data_to_insert)
    conn.commit()
                    
async def executor(domains):
    
    cursor = conn.cursor()
    for domain in domains:
        print(domain)
        try:
            await main(domain, cursor)
        except Exception as error:
            print('got error:',error)
            traceback.print_exc(file=sys.stdout)
    cursor.close()
    conn.close()


tempurls =["https://czapski.art.pl/","https://fida.pk","https://hackesta.org","https://www.webskysolutions.com", "https://www.wastaffingllc.com", "https://faharistaffing.com", "https://www.premierestaffing.biz", "https://www.seekcareers.com", "https://www.abicsl.com/", "https://copelandstaffingandrecruiting.com/", "https://ehospitalhire.com/", "https://www.staffingplusjobs.com/", "https://talentteam.com/contact-us/","https://x3tradesmen.com/","https://www.greencountrystaffing.com/","https://www.positiveresultsstaffing.com/", "http://allstaffusa.com/", "http://www.dennisrwencel.com/ContactUs.html","https://infobankmediaseo.com/", "https://www.cashregisterguys.com/","https://www.julieawhelancpallc.com/", "http://www.rangeus.com/", "https://helpdesk.bitrix24.com/", "https://fedeleandassociates.com/", "https://emergingbusinessservices.com/", "https://www.sagebooksinc.com/", "https://knowink.com/", "https://www.innovativetimingsystems.com/", "http://corptechs.com/","https://www.simonswerk.com/service/contact","https://stonemountaintoyota.com/","https://www.cheyenneregional.org/","https://www.paivafinancial.com/","https://sciodpm.com/","https://mstaxprep.com/","https://www.redpointglobal.com/","https://www.frankhajekandassociates.com/","http://www.trif.com/","https://www.m-csi.com/","http://kkdesignonline.com/","http://www.ncomm.com/ncomm/","https://telebright.com/","https://www.bdkinc.com/"]
 
asyncio.get_event_loop().run_until_complete(executor(["https://www.bdkinc.com/"]))


#====================================================== NOTES ============================================================
# columns to get data for :
# ✔ tld 
#   name
# ✔ contacturl
# ✔ abouturl
# ✔ leadershipurl
# ✔ careersurl
# ✔ facebookurl
# ✔ twitterurl
# ✔ instagramurl
# ✔ linkedinurl
# ✔ isformonhp
# ✔ isformoncp
#   isindiaoncp
#   hometitle
# ✔ homekeywords
# ✔ homedescription
# ✔ twitterhandle
#   lastcrawldate
#   ccrawleddate
#   ucategory
#   status
#   gmapsurl
# ✔ phones
# ✔ emails
# ✔ addresses
#   pcodes 
# ✔ people
#   isgoodurl
# ✔ ogtitle
# ✔ ogdescription
# ✔ ogurl
# ✔ ogimage
# ✔ oglocale
# ✔ ogsitename
# ✔ language
#   isparked
# ✔ isenglish
#   youtubeurl
#   pinteresturl

