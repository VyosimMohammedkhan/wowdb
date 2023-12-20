//==================================================== dependencies ==========================================================

const mysql = require('mysql2/promise')
const fs = require('fs');
const cheerio = require('cheerio');
const extractor = require("phonenumbers-extractor");

const { fullTitles, acronymTitles, roletitles, xpath_alternate, stopwords, links } = require('./datastore')

const pool = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'wowdb',
});

//==================================================== Main function ==========================================================

async function main(domainpath) {

    let people = []
    let phones = []

    const files = fs.readdirSync(domainpath)

    for (let file of files) {

        const htmlData = fs.readFileSync(`${domainpath}/${file}`, 'utf-8');
        const $ = cheerio.load(htmlData);

        // if (file.match(/about|team/)) {

        //     people = await getPeople($, roletitles) 
        //     if(people.length>0){ 
        //         console.log(domainpath); 
        //         await sendDataToDB(domainpath.split('/').pop(), people)
        //     }

        // }

        if (file.match(/about|contact/)) {
            const text = $('body').text().replace(/\s+/g, ' ')
            phones = extractor.extractNumbers(text, 6);
            if (phones) console.log(new Set(phones))
        }
    }


}

//==================================================== Helper functions ==========================================================


async function getPeople($, titles) {

    let uniquenames = new Set();
    let possibletitles = []
    let people = []
    let person = {}


    for (let title of titles) {
        possibletitles = $('*:not(form)')
            .filter(function () {
                const text = $(this).text().replace(/\s+/, ' ').trim().toLowerCase();
                return (
                    text.match(new RegExp(`\\b${title}\\b`)) &&
                    text.length < 60
                );
            });

        for (let t of possibletitles) {
            person = await getPerson($, t, titles)

            const nameHasRole = titles.some(role => person.name.toLowerCase().includes(role));
            const nameHasStopword = stopwords.some(stopword => person.name.toLowerCase().includes(stopword));


            if (person.role && person?.name?.length < 30 &&
                !person?.name?.match(/[^a-zA-Z\s.'’,-]/) &&
                !nameHasRole && !nameHasStopword &&
                person?.name?.includes(' ') &&
                !uniquenames.has(person.name))

                people.push(person); uniquenames.add(person.name);
        }

    }

    return people
}

async function getPerson($, roleElement, roletitles) {

    let personrole = $(roleElement).text();
    let personname = $(roleElement).prev().text();
    let parentnode = $(roleElement).parent();
    let parentText = parentnode.text();

    function getNameBySplittingWith(char, name, role) {
        const [tempname, temprole] = role.split(char).filter(el => el);

        const nameHasRole = roletitles.some(role => tempname.toLowerCase().includes(role));
        const nameHasStopword = stopwords.some(stopword => tempname.toLowerCase().includes(stopword));

        if (!nameHasRole && !nameHasStopword && !tempname.match(/[^a-zA-Z\s.'’,-]/)) {
            return [tempname, temprole];
        } else {
            return [name, role];
        }
    }

    if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes('–')) {
        [personname, personrole] = getNameBySplittingWith('–', personname, personrole);
    }

    if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes('-')) {
        [personname, personrole] = getNameBySplittingWith('-', personname, personrole);
    }

    if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes('|')) {
        [personname, personrole] = getNameBySplittingWith('|', personname, personrole);
    }

    if ((!personname || personname.length > 30 || !personname.includes(' ')) && personrole.includes(',')) {
        [personname, personrole] = getNameBySplittingWith(',', personname, personrole);
    }

    if (!personname || personname.length > 30 || !personname.includes(' ')) {
        while (parentText === personrole) {
            parentnode = parentnode.parent();
            parentText = parentnode.text();
        }

        personname = parentText.replace(personrole, '');
    }

    person = { role: personrole?.replace(/\s+/g, ' ').trim(), name: personname?.replace(/\s+/g, ' ').trim() };

    return person
}


//================================================== DB functions =============================================================

async function sendDataToDB(url, people) {

    const deletequery = `delete from people_cheerio where path='${url}'`
    const insertquery = `INSERT IGNORE INTO people_cheerio ( path, people ) VALUES ( ?, ? );`

    // let values = []
    // try { await pool.execute(deletequery) } catch (err) { console.log(err) }
    try { await pool.execute(insertquery, [url, people]) } catch (err) { console.log(err) }

    // for (let person of people) {
    //     values = [url, person.name, person.role]
    //     try {
    //         await pool.execute(insertquery, values);
    //     } catch (err) {
    //         console.log(err)
    //     }
    // }

}


//======================================================= Surya Sirs method ==================================================

//         const aStopWords = ['http', 'virtual', 'conclave', 'summit', 'conference'];
//         const aTitles = ['chairman', 'president', 'chief.*officer', 'c.{1,2}o', 'senior leader', 'senior lead', 'delivery leader', 'delivery lead', 'delivery head', 'director', 'head of', 'entrepreneur', 'auditor', 'managing partner', 'partner'];

//===================================================== Execution ===========================================================


async function executer() {
    const path = "/home/dell/Documents/Mohammed Backup/offline data"
    const domains = fs.readdirSync(path)

    for (let domain of domains) {
        // console.log(domain)
        try {
            await main(`${path}/${domain}`)
        } catch (err) {
            console.log("caught error for ", domain, err)
        }

    }
}

executer()
// main()