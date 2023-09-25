# 's' for session
# 'r'' for response
# 'doctionary' contains (filenames, links)

import os, re, csv
import codecs, string
import requests
import shutil
import json, time
from lxml import html
from collections import OrderedDict
from inspect import currentframe, getframeinfo, stack
from subprocess import check_call, CalledProcessError
from datetime import datetime, timedelta
import argparse
import sys
import traceback
from documents import DocumentFile

class PesticideScraper:

    def __init__(self):
        self.s = requests.session()
        self.path = os.getcwd() + '/Outputs'
        print("Creating data directory under {}".format(self.path))
        time.sleep(0.5)

        try:
            today = datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S")
            os.mkdir(self.path + '/' + today)
            os.mkdir(self.path + '/' + today + '/pesticides')
            os.mkdir(self.path + '/' + today + '/adjuvants')
            os.mkdir(self.path + '/' + today + '/extensions')
            self.path += '/' + today

        except OSError as e:
            print("Error reseting directories: {}".format(e))
            time.sleep(3)

        else:
            print("Output folders created")
            time.sleep(1.5)

        self.load_exceptions()

    def load_exceptions(self):

        reader = csv.reader(open('exception_pesticides.csv', 'r'))
        self.pesticides_exceptions = []
        for row in reader:
            self.pesticides_exceptions.append(row[0].lower())

    def checkStatus(self, response, url):
        frameinfo = getframeinfo(stack()[1][0])
        timenow = datetime.now()
        dt_string = timenow.strftime("%d/%m/%Y %H:%M:%S")
        if response.status_code != 200:
            with open('error.log', 'a') as e:
                e.write(dt_string + ' ' + frameinfo.filename + ' [{0}]:'.format(str(frameinfo.lineno + 3)))
                e.write('\n\t\t(' + url + ')  [Status {0}]'.format(str(response.status_code)) + '\n' )
            return False
        else:
            return True

    def getPesticideLinks(self, target):

        Links = []
        lastpage = 1
        regex = re.compile(".*pageno=(\d+).*")
        timenow = datetime.now()
        self.date = timenow.strftime("%d/%m/%Y")

        if target == 'All':

            contains = 'getfullproduct'
            url 	 = "https://secure.pesticides.gov.uk/pestreg/ProdList.asp"
            payload  = {
                        'origin': 'prodsearch',
                        'product': '%',
                        'resultsperpage': '100',
                        'submit_btn': 'Get Results'
                        }

        else:

            yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%d/%m/%Y')
            contains = 'getfullproduct'
            url  	 = 'https://secure.pesticides.gov.uk/pestreg/PMList.asp'
            payload  = {
                        'origin': 'pmsearch',
                        'modifieddate': yesterday,
                        'resultsperpage': '100',
                        'submit_btn': 'Get Results'
                        }


        r = self.s.post(url, data=payload)
        self.checkStatus(r, url)
        parsed = html.fromstring(r.text)

        ## Find number of pages to loop
        endlink = parsed.xpath("//p[@class='result-nav']//a[contains(text(), 'End')]/@href")
        if len(endlink) > 0:
            lastpage = int(re.search(regex, endlink[0]).group(1))
        else:
            beforenext = parsed.xpath("//p[@class='result-nav']//a[contains(text(), 'Next')]/preceding::a[1]/@href")
            if len(beforenext) > 0:
                lastpage = int(re.search(regex, beforenext[0]).group(1))

        ## Loop through pages and gather links
        for i in range(1, lastpage + 1): # lastpage + 1  |  2
            time.sleep(0.2)
            r = self.s.get(url+"?pageno={0}".format(i))
            self.checkStatus(r, url+"?pageno={0}".format(i))
            parsed = html.fromstring(r.text)

            if target == 'All':
                Links += parsed.xpath("//td[@class='db']//a[contains(@href, '{0}')]/@href".format(contains))
            else:
                trs = parsed.xpath("//tbody/tr")
                for tr in trs:
                    link = tr.xpath("td[@class='db']//a[contains(@href, '{0}')]/@href".format(contains))
                    if not link:
                        continue
                    link = link[0]
                    notice_type = tr.xpath('td[last()]/text()')
                    notice_type = notice_type[0].replace('\n', '').replace('\t', '').replace('\t',
                                                                                             '').strip() if notice_type else "Monthly Revocation Notice"
                    if notice_type == "":
                        notice_type = "Monthly Revocation Notice"

                    file_link = None
                    if notice_type not in ['Authorisation', 'Correction']:
                        approval_date = tr.xpath("td[@class='db']//a[contains(@href, 'getfile.asp')]/text()")

                        approval_date = approval_date[0].\
                            replace('\n', '').replace('\t', '').replace('\t', '').strip() if approval_date else ''

                        file_link = tr.xpath("td[@class='db']//a[contains(@href, 'getfile.asp')]/@href")
                        file_link = f'{file_link[0]}&approvalno={approval_date}' if file_link else None

                    Links.append({
                        'link': link,
                        'notice_type': notice_type,
                        'file_link': file_link
                    })
        return Links

    def get_crops(self, text):

        if text.lower() in self.pesticides_exceptions:
            print("Exception pesticide found for Pesticide_Scraper.py:", text)
            return [text]

        while '\n\n\n' in text:
            text = text.replace('\n\n\n', '\n\n')

        if text.lower() in self.pesticides_exceptions:
            print("Exception pesticide found for Pesticide_Scraper.py:", text)
            return [text]

        crops = []

        text = text.replace('\n\n', ',')
        texts = text.split(' and ')

        for text in texts:
            ini = 0
            pause = False
            for i, c in enumerate(text):
                if c == ',' and not pause:
                    crops.append(text[ini:i].strip().capitalize())
                    ini = i + 1
                elif c == '(':
                    pause = True
                elif c == ')':
                    pause = False

            if text[ini:].strip():
                    crops.append(text[ini:].strip().capitalize())

        return crops

    def getPesticides(self, link, cont=0, update=False, notice_type=None, file_link=None):

        os.chdir(self.path)
        dictionary = {}
        doctionary = {}
        actives = []
        regex = re.compile(".*documentid=(\d+).*")
        url = 'https://secure.pesticides.gov.uk/pestreg/' + link
        try:
            r = self.s.get(url)
        except ConnectionError:
            time.sleep(10)
            cont += 1
            if cont < 4:
                return self.getPesticides(link, cont, update=update, notice_type=None, file_link=None)
            else:
                raise Exception("Connection error more than 3 times")

        self.checkStatus(r, url)
        parsed = html.fromstring(r.text)

        InfoTable = parsed.xpath("//table[@class='db']//tr")


        # Add (Url - value) field
        dictionary.update({'Url': url, 'flags':[]})

        # Grab all (field - value) rows from product's Detail Page
        for i in range(len(InfoTable)):

            if InfoTable[i][1].text is not None:
                dictionary.update({InfoTable[i][0].text.replace('\t', '').replace('\r', '').replace('\n', '').replace(':', '').strip():InfoTable[i][1].text.replace('\t', '').replace('\r', '').replace('\n', '').strip()})
                if "Replacement Products:" in InfoTable[i][0].text:
                    dictionary.update({InfoTable[i][0].text.replace('\t', '').replace('\r', '').replace('\n','').replace(':','').strip():InfoTable[i][1].xpath('text()[2]')[0].replace('\t', '').replace('\r', '').replace('\n','').strip()})
            else: # In case field value is None
                dictionary.update({InfoTable[i][0].text.replace('\t', '').replace('\r', '').replace('\n', '').replace(':', '').strip(): ''})

        # Clean dictionary from unnecessary (field - value) rows
        for field, value in dictionary.copy().items():

            if field not in ['Url', 'MAPP (Reg.) Number', 'Product Name', 'Authorisation Holder', 'Marketing Company', 'First Authorisation Date', 'Product Expiry Date', 'Formulation Type', 'Field of Use', 'Amateur / Professional', 'LERAP Category', 'Aquatic Use', 'Authorisation Level', 'Active Substance(s)', 'Aerial Use', 'Parallel Import', 'Replacement Products', 'flags', 'Crops', 'Low Risk Product']:
                if not (notice_type and field == 'Extent of Authorisation'):
                    del dictionary[field]
            #if field == 'Authorisation Level' and 'Product also has' in dictionary[field]:
                #dictionary[field] = ''
            if field == 'LERAP Category' and 'n/a' in dictionary[field]:
                dictionary[field] = ''
            elif field == 'LERAP Category' and 'See Authorisation for' in dictionary['LERAP Category']:
                dictionary[field] = " ".join(str(dictionary[field]).split())
            elif field == 'Crops':
                dictionary[field] = self.get_crops(dictionary[field])

        if 'Crops' in dictionary:
            dictionary['Web Crops'] = dictionary.pop('Crops')

        # Delete empty keys
        delete_if_empty = ['Field of Use', 'LERAP Category']
        for empty_field in delete_if_empty:
            if dictionary.get(empty_field) == '':
                del dictionary[empty_field]

        if 'Product also has' in dictionary.get('Authorisation Level', ''):
            dictionary['Authorisation Level'] += ' Extensions of Authorisation. (opens new window)'
        actives = []
        active_substances = []
        if len(dictionary.get('Active Substance(s)', [])) > 0:
            actives_first = dictionary['Active Substance(s)'].split(' and ')
            for a in actives_first:
                actives += a.split(', ')

            for a in actives:
                pos1 = a.find('/')
                if a[pos1 + 1] == " ":
                    pos2 = a.find(' ', pos1 + 2)
                else:
                    pos2 = a.find(' ', pos1)

                if a[pos1 - 1] == " ":
                    pos3 = a[:pos1 - 2].rfind(" ")
                else:
                    pos3 = a[:pos1 - 1].rfind(" ")

                if a[pos3 - 1] == "%":
                    pos3 -= 1

                substance = a[pos2:].strip()
                value = a[:pos3].strip()
                metric = a[pos3:pos2].strip()

                try:
                    int(value.replace('.', ''))
                except:
                    if 'x10' not in value:
                        if len(active_substances) > 0:
                            active_substances[-1]['substance'] += ', ' + a.strip()
                            continue

                if not update:
                    if value and (not metric or metric == '-'):
                        dictionary['flags'].append('Active substance with no metric found: {} ({})'.format(substance, value))

                active_substances.append({'value': value, 'metric': metric, 'substance': substance})

        # Format replacement products
        if len(dictionary.get('Replacement Products', [])) > 0:
            products = dictionary['Replacement Products'].split(",")
            replacements = []
            for product in products:
                product = product.lstrip()
                rep = {"Product Name": product.split(' (')[0] ,"MAPP (Reg.) Number": product.split('(')[1].split(')')[0]}
                replacements.append(rep)

            dictionary['Replacement Products'] = replacements

        dictionary['Active Substance(s)'] = active_substances

        if update:
            set_flags = ['Authorisation Holder', 'Marketing Company', 'Formulation Type']
        else:
            set_flags = ['Active Substance(s)', 'Authorisation Holder', 'Marketing Company', 'Formulation Type']

        for f in set_flags:
            if not dictionary.get(f):
                dictionary['flags'].append('Pesticide does not have {}'.format(f))


        # Gather Available Notices links
        if file_link:
            doctionary.update({file_link[-8:] + '-'+re.search(regex, file_link).group(1) + '-' + notice_type + '.doc': 'https://secure.pesticides.gov.uk/pestreg/' + file_link})
        else:
            doclinks = parsed.xpath("//td[@class='db']//a[contains(@href, 'getfile')]/@href")
            doctypeBefore = parsed.xpath("//text()[preceding-sibling::a[contains(@href, 'getfile')]]")
            doctypeAfter = []

            # Then Clean text around (Document Type)
            for text in doctypeBefore:
                txt = text.replace('\t', '').replace('\r', '').replace('\n', '').replace(',', '').replace('(', '').replace(')', '').strip()
                doctypeAfter.append(txt)

            for i in range(len(doclinks)):
                doctionary.update({doclinks[i][-8:]+'-'+re.search(regex, doclinks[i]).group(1)+'-'+doctypeAfter[i]+'.doc':'https://secure.pesticides.gov.uk/pestreg/'+doclinks[i]})

        # If Expired Notices link exists, follow it
        link2expired = parsed.xpath("//td[@class='db']//a[contains(@href, 'ExpiredNotices')]/@href")
        if len(link2expired) > 0:
            r = self.s.get('https://secure.pesticides.gov.uk/pestreg/'+link2expired[0])
            self.checkStatus(r, 'https://secure.pesticides.gov.uk/pestreg/'+link2expired[0])
            parsed = html.fromstring(r.text)

            # And gather all Expired Notices documents (procedure simlar as before)
            doclinks = parsed.xpath("//td[@class='db']//a[contains(@href, 'getfile')]/@href")
            doctypeBefore = parsed.xpath("//text()[preceding-sibling::a[contains(@href, 'getfile')]]")
            doctypeAfter = []

            # Then Clean text around (Document Type)
            for text in doctypeBefore:
                txt = text.replace('\t', '').replace('\r', '').replace('\n', '').replace(',', '').replace('(', '').replace(')', '').strip()
                doctypeAfter.append(txt)

            for i in range(len(doclinks)):
                doctionary.update({doclinks[i][-8:]+'-'+re.search(regex, doclinks[i]).group(1)+'-'+doctypeAfter[i]+'-Expired'+'.doc':'https://secure.pesticides.gov.uk/pestreg/'+doclinks[i]})

        # Paralell products
        get_parallel_parent = False
        parallel = dictionary.get('Parallel Import', '').strip()
        if parallel == 'Yes':
            json_file = open(self.path + '/../../parallel_mapping.json', 'r')
            mapping = json.loads(json_file.read())
            json_file.close()
            dictionary['parallel_parent'] = mapping.get(dictionary['MAPP (Reg.) Number'], {'name': 'Not found', 'mapp': 'Not found'})
            if dictionary['parallel_parent']['name'] == 'Not found':
                get_parallel_parent = True
        elif 'Yes' in parallel:
            dictionary['Parallel Import'] = 'Yes'
            get_parallel_parent = True

        # Create and cd into each Record Folder
        try:
            os.mkdir(self.path + '/pesticides/' + dictionary['MAPP (Reg.) Number'])
        except OSError as e:
            print("Error creating directory: {}".format(e))

        os.chdir(self.path + '/pesticides/' + dictionary['MAPP (Reg.) Number'])

        # For update only
        if notice_type:
            dictionary['Notice Type'] = notice_type

        # Download all documents in /pesticides/recordname
        # with recordname = MAPP (Reg.) Number
        i = 0
        files_map = {}
        for name, dlink in doctionary.items():

            if os.path.exists(name) or os.path.exists(name + 'x'):
                continue

            r = self.s.get(dlink, stream=True)

            d = r.headers['content-disposition']
            extension = re.findall("filename=.+\.(\w+)", d)[0]
            name = '.'.join([name.split('.')[0], extension])

            if self.checkStatus(r, dlink):
                download = open(name, "wb")

                for chunk in r.iter_content(chunk_size=256):
                    if chunk:
                        download.write(chunk)
                i += 1

                files_map[name] = dlink

                download.close()

                time.sleep(0.2)

        docs, substances = DocumentFile.from_mapp_folder(get_parent=get_parallel_parent, map=files_map)

        for k, v in docs.items():
            if k == 'flags':
                dictionary[k] += v
            else:
                dictionary[k] = v

        if 'Yes' in parallel:
            if dictionary.get('parallel_parent', {}).get('name') == 'Not found' and dictionary.get('parallel_parent', {}).get('name') == 'Not found':
                dictionary['flags'].append('Parallel parent information Not Found')

        if not dictionary['flags']:
            del dictionary['flags']

        return dictionary, substances

    def check_date(self, date_str, big_dict, key):

        if not date_str:
            big_dict['flags'].append("A value for date was not found")
            big_dict[key] = 'Invalid Date'

            return big_dict

        new_date_str = date_str.replace(' ', '').replace('for', '').replace('except', '').strip()
        possible_formats = ["%d/%m/%Y", "%d%B%Y", "%dst%B%Y", "%dth%B%Y"]

        for f in possible_formats:
            try:
                date_obj = datetime.strptime(new_date_str, f)
                big_dict[key] = date_obj.strftime("%d/%m/%Y")

                return big_dict
            except ValueError:
                continue

        big_dict['flags'].append("The following date is not in the right format: {}".format(date_str))
        big_dict[key] = 'Invalid Date'

        return big_dict

    def getExtensions(self, target):

        Extensions = {}
        lastpage = 1
        regex = re.compile(".*pageno=(\d+).*")
        timenow = datetime.now()
        self.date = timenow.strftime("%d/%m/%Y")

        if target == 'All':
            url = "https://secure.pesticides.gov.uk/offlabels/OffLabelList.asp"
            payload  = {
                        'origin': 'search',
                        'active': '%',
                        'submit_btn': 'Get Results',
                        'resultsperpage': '100'
                        }
        else:
            yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%d/%m/%Y')
            url = "https://secure.pesticides.gov.uk/offlabels/OffLabelList.asp"
            payload = {
                'origin': 'search',
                'active': '%',
                'modifieddate': yesterday,
                'submit_btn': 'Get Results',
                'resultsperpage': '100'
            }

        r = self.s.post(url, data=payload)
        self.checkStatus(r, url)
        parsed = html.fromstring(r.text)

        ## Find number of pages to loop
        endlink = parsed.xpath("//p[@class='result-nav']//a[contains(text(), 'End')]/@href")
        if len(endlink) > 0:
            lastpage = int(re.search(regex, endlink[0]).group(1))
        else:
            beforenext = parsed.xpath("//p[@class='result-nav']//a[contains(text(), 'Next')]/preceding::a[1]/@href")
            if len(beforenext) > 0:
                lastpage = int(re.search(regex, beforenext[0]).group(1))

        os.chdir(self.path + '/extensions/')

        ## Loop through pages and gather links
        for i in range(2, lastpage + 2):  # lastpage + 1  |  2

            print("Page", i-1, "of", lastpage)
            rows = parsed.xpath("//table[@class='dbresult']/tbody/tr")
            for row in rows:
                extension = {'flags': []}
                auth_number = 'Unnamed'
                col2 = row.xpath('td[3]')
                for c in col2:
                    auth_number = c.xpath('a/strong')[0].text
                    extension['auth_number'] = auth_number
                    extension['doc_link'] = 'https://secure.pesticides.gov.uk/offlabels/' + c.xpath('a')[0].get('href')
                    extension = self.check_date(c.xpath('br')[0].tail.strip(), extension, 'issue')
                    extension = self.check_date(c.xpath('br')[1].tail.strip(), extension, 'expiry')

                col1 = row.xpath('td[1]')
                for c in col1:
                    extension['mapp'] = c.xpath('br')[0].tail.strip()

                col3 = row.xpath('td[2]/text()')
                if col3:
                    extension['Extent of Authorisation'] = col3[0].strip()

                col7 = row.xpath('td[7]')
                if col7:
                    web_pests = col7[0].text
                    for child in col7[0].getchildren():
                        web_pests += f'{child.text} {child.tail}'
                    web_pests = web_pests.strip()
                    if web_pests:
                        extension['Web Pests'] = web_pests
                        extension['Web Pests'] = extension['Web Pests'].split(', ')

                # Download all documents in /pesticides/recordname
                # with recordname = MAPP (Reg.)

                dlink = extension['doc_link']
                r = requests.get(dlink, stream=True)

                d = r.headers['content-disposition']
                file_extension = re.findall("filename=.+(\.\w+)", d)[0]

                if self.checkStatus(r, dlink):

                    name = auth_number + file_extension
                    download = open(name, "wb")

                    for chunk in r.iter_content(chunk_size=256):
                        if chunk:
                            download.write(chunk)

                    download.close()

                    time.sleep(0.2)

                    try:
                        file = DocumentFile(name, True)
                        doc = file.read_docx()
                    except:
                        try:
                            file = DocumentFile(name, True)
                            doc = file.read_docx()
                        except:
                            print("Failed converting file to docx")
                    else:
                        file.get_main_info(doc)
                        protections = file.get_protections(doc)
                        extension['protections'] = {"protections":protections,
                                         "date": file.date_of_issue if hasattr(file, 'date_of_issue') else None,
                                         "file": file.path}
                        crops, aquatic = file.get_crops(doc)
                        if crops:
                            extension['crops'] = crops

                        if aquatic:
                            extension['aquatic'] = aquatic

                        if file.flags:
                            extension['flags'] += file.flags


                set_flags = ['mapp', 'crops']
                for f in set_flags:
                    if not extension.get(f):
                        extension['flags'].append('Extension does not have {}'.format(f))


                if not extension['flags']:
                    del extension['flags']

                Extensions[auth_number] = extension

            with open("../extensions.json", "w") as file:
                file.write(json.dumps(Extensions))

            if i >= lastpage:
                break

            time.sleep(0.2)
            r = self.s.get(url + "?pageno={0}".format(i))
            self.checkStatus(r, url + "?pageno={0}".format(i))
            parsed = html.fromstring(r.text)

        with open("../extensions.json", "w") as file:
            file.write(json.dumps(Extensions, indent=2))

        return Extensions

    def getAdjuvantLinks(self, target):

        out_dir = '/'.join(self.path.split('/')[:-2])
        os.chdir(out_dir)

        if target == "All":
            url 	 = "https://secure.pesticides.gov.uk/adjuvants/Search.aspx"
            r = self.s.get(url)
            self.checkStatus(r, url)

            parsed = html.fromstring(r.text)
            viewstate = str(parsed.xpath("//input[@name='__VIEWSTATE']/@value")[0])
            stategen  = str(parsed.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value")[0])
            eventvalid = str(parsed.xpath("//input[@name='__EVENTVALIDATION']/@value")[0])
            time.sleep(1)

            payload  = {
                        'ctl00$ContentPlaceHolder1$ToolkitScriptManager2': 'ctl00$ContentPlaceHolder1$pnlOuter|ctl00$ContentPlaceHolder1$SearchBtn',
                        'ContentPlaceHolder1_ToolkitScriptManager2_HiddenField': '',
                        '__EVENTTARGET': '',
                        '__EVENTARGUMENT': '',
                        '__VIEWSTATE': viewstate,
                        '__VIEWSTATEGENERATOR': stategen,
                        '__EVENTVALIDATION': eventvalid,
                        'ctl00$ContentPlaceHolder1$AdjuvantTb': '',
                        'ctl00$ContentPlaceHolder1$MappTb': '',
                        'ctl00$ContentPlaceHolder1$ApplicantTb': '',
                        'ctl00$ContentPlaceHolder1$eacgTb': '',
                        'ctl00$ContentPlaceHolder1$activeTb': '',
                        'ctl00$ContentPlaceHolder1$ProductTb': '',
                        'ctl00$ContentPlaceHolder1$CropTb': '',
                        '__ASYNCPOST': 'true',
                        'ctl00$ContentPlaceHolder1$SearchBtn': 'Get Results'
                        }

            r = self.s.post(url, data=payload, headers={
                                                        'Cache-Control': 'no-cache', \
                                                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', \
                                                        'Origin': 'https://secure.pesticides.gov.uk', \
                                                        'Sec-Fetch-Mode': 'cors', \
                                                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36', \
                                                        'DNT': '1', \
                                                        'X-MicrosoftAjax': 'Delta=true', \
                                                        'X-Requested-With': 'XMLHttpRequest', \
                                                        'Referer': 'https://secure.pesticides.gov.uk/adjuvants/Search.aspx'})

            self.checkStatus(r, url)

            time.sleep(1.18)
            r = self.s.get("https://secure.pesticides.gov.uk/adjuvants/AdjuvantList.aspx")
            self.checkStatus(r, url)
            parsed = html.fromstring(r.text)
            ResponseStatus = r.status_code
            viewstate = str(parsed.xpath("//input[@name='__VIEWSTATE']/@value")[0])
            stategen = str(parsed.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value")[0])
            eventvalid = str(parsed.xpath("//input[@name='__EVENTVALIDATION']/@value")[0])
            links = parsed.xpath("//a[contains(@href, 'ListEntry')]/@href")

            i = 1
            while ResponseStatus == 200:
                time.sleep(1)
                i += 1
                payload  = {
                            'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$AdjuvantGV',
                            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$AdjuvantGV',
                            '__EVENTARGUMENT': 'Page${0}'.format(i),
                            '__VIEWSTATE': viewstate,
                            '__VIEWSTATEGENERATOR': stategen,
                            '__EVENTVALIDATION': eventvalid,
                            '__ASYNCPOST': 'true'
                            }
                time.sleep(1)
                r = self.s.post("https://secure.pesticides.gov.uk/adjuvants/AdjuvantList.aspx", data=payload, headers={

                                                        'Cache-Control': 'no-cache', \
                                                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', \
                                                        'Origin': 'https://secure.pesticides.gov.uk', \
                                                        'Sec-Fetch-Mode': 'cors', \
                                                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36', \
                                                        'DNT': '1', \
                                                        'X-MicrosoftAjax': 'Delta=true', \
                                                        'X-Requested-With': 'XMLHttpRequest', \
                                                        'Referer': 'https://secure.pesticides.gov.uk/adjuvants/AdjuvantList.aspx'})

                self.checkStatus(r, "https://secure.pesticides.gov.uk/adjuvants/AdjuvantList.aspx")
                parsed = html.fromstring(r.text)

                if 'Adjuvants Error Page' in r.text:
                    print("End of Adjuvant")
                    break

                ResponseStatus = r.status_code
                viewstate = re.search("__VIEWSTATE\|(.*?)\|", r.text)
                stategen = re.search("__VIEWSTATEGENERATOR\|(.*?)\|", r.text)
                eventvalid = re.search("__EVENTVALIDATION\|(.*?)\|", r.text)
                viewstate = viewstate.group(1)
                stategen = stategen.group(1)
                eventvalid = eventvalid.group(1)

                links = links + parsed.xpath("//a[contains(@href, 'ListEntry')]/@href")

            return links

        else:
            time.sleep(1.18)
            url = 'https://secure.pesticides.gov.uk/adjuvants/updates.aspx'
            r = self.s.get(url)
            self.checkStatus(r, url)
            parsed = html.fromstring(r.text)
            rows = parsed.xpath("//table[@id='ContentPlaceHolder1_tblLenums']/tr")

            additions = []
            removals = {}
            todays_json_output = {'additions': [], 'removals': []}

            if os.path.isfile('yesterdays_adjuvants.json'):
                outfile = open('yesterdays_adjuvants.json', 'r')
                yesterdays_json_output = json.load(outfile)
            else:
                yesterdays_json_output = {}


            for row in rows:
                h1 = row.xpath('td/h1')
                h2 = row.xpath('td/h2')
                adj = row.xpath('td[2]')

                if len(h1) > 0:
                    break
                elif len(h2) > 0:
                    if "Additions" in h2[0].text:
                        add = True
                        rem = False
                    elif "Removals" in h2[0].text:
                        add = False
                        rem = True
                elif len(adj) > 0:
                    if add or rem:
                        blank = row.xpath('(td[2]/text()) | (td[2]/a/text())')
                        if not blank:
                            continue
                    else:
                        continue

                    if add:

                        id = str(row.xpath('td[@class="listentry"]/a/text()|td[@class="listentry"]/text()')[0])
                        todays_json_output['additions'].append(id)

                        if id not in yesterdays_json_output.get('additions', [id]):
                            link = row.xpath('td[@class="listentry"]/a/@href')
                            if link:
                                additions.append(link[0])

                    elif rem:

                        id = str(row.xpath('td[1]/text()')[0])
                        todays_json_output['removals'].append(id)

                        if id not in yesterdays_json_output.get('removals', [id]):
                            name = row.xpath('td[2]/text()')[0]
                            number = row.xpath('td[3]/text()')[0]
                            rem_date = row.xpath('td[6]/text()')[0]
                            removals[number] = {"name": name, "number": number, "removal_date": rem_date}

            outfile = open('yesterdays_adjuvants.json', 'w')
            json.dump(todays_json_output, outfile)

            return additions, removals

    def getAdjuvantInfo(self, link):

        cropdict = {}
        dictionary = OrderedDict()
        orddict = OrderedDict()

        CropTable = []
        InAddition = ''

        subslist = []

        regex = re.compile('(\d+\.?\d*)\s(.*/..)(.*$)')

        url = 'https://secure.pesticides.gov.uk/adjuvants/' + str(link)
        r = self.s.get(url)
        self.checkStatus(r, url)
        parsed = html.fromstring(r.text)

        dictionary.update({'Url': url})
        dictionary.update({'flags': []})
        dictionary.update({'Name': parsed.xpath("//span[@id='ContentPlaceHolder1_lblAdjuvantName']//text()")[0]})
        dictionary.update({'Number': parsed.xpath("//span[@id='ContentPlaceHolder1_lblAdjuvantNumber']//text()")[0]})
        dictionary.update({'Formulation':
                               parsed.xpath("//span[@id='ContentPlaceHolder1_lblFormulation']/text()")[0].split(
                                   'containing')[0]})
        dictionary.update({'Applicant': str(parsed.xpath("//span[@id='ContentPlaceHolder1_lblApplicant']//text()")[0]).split(',')[0]})
        dictionary = self.check_date(parsed.xpath("//span[@id='ContentPlaceHolder1_lblIncDate']//text()")[0], dictionary, 'Date of Inclusion')
        dictionary.update({'Field of Use': parsed.xpath("//span[@id='ContentPlaceHolder1_lblFOU']//text()")[0]})

        substances = parsed.xpath("//span[@id='ContentPlaceHolder1_lblFormulation']/text()")[0].split('containing')[1].split(' (detailed')[0].strip()

        actives = []
        active_substances = []
        if len(substances) > 0:
            actives_first = substances.split(' and ')
            for a in actives_first:
                actives += a.split(', ')

            for a in actives:
                pos1 = a.find('/')
                if a[pos1 + 1] == " ":
                    pos2 = a.find(' ', pos1 + 2)
                else:
                    pos2 = a.find(' ', pos1)

                if a[pos1 - 1] == " ":
                    pos3 = a[:pos1 - 2].rfind(" ")
                else:
                    pos3 = a[:pos1 - 1].rfind(" ")

                if a[pos3 - 1] == "%":
                    pos3 -= 1

                substance = a[pos2:].strip()
                value = a[:pos3].strip()
                metric = a[pos3:pos2].strip()

                try:
                    int(value.replace('.', ''))
                except:
                    if 'x10' not in value:
                        if len(active_substances) > 0:
                            active_substances[-1]['substance'] += ', ' + a.strip()
                            continue

                active_substances.append({'value': value, 'metric': metric, 'substance': substance})

        dictionary['Formulation Substances'] = active_substances

        dictionary['CropInfo'] = []
        CropTable = CropTable + parsed.xpath("//table[@id='ContentPlaceHolder1_tblUses']//tbody//tr")
        for i in range(len(CropTable)):
            more = CropTable[i][0].text.split(',')
            muchmore = more
            for m in more:
                if ' and ' in m:
                    muchmore.remove(m)
                    muchmore = muchmore + m.split(' and ')
            for m in muchmore:
                orddict = OrderedDict()
                orddict['Crop'] = m.strip()
                pesticide = CropTable[i][1].text.strip()
                if '(MAPP' in pesticide:
                    pos = pesticide.find('(MAPP')
                    orddict['Pesticide'] = pesticide[:pos].strip()
                    mapp = pesticide[pos +  5:].strip()
                    pos = mapp.find(')')
                    orddict['MAPP'] = mapp[:pos].strip()

                else:
                    orddict['Pesticide Category'] = pesticide

                orddict['Maximum Concentration'] = CropTable[i][2].text.strip()
                orddict['Maximum number of treatments'] = CropTable[i][3].text.strip()
                orddict['Latest time of application'] = CropTable[i][4].text.strip()
                dictionary['CropInfo'].append(orddict)

        InAddition = parsed.xpath("//span[@id='ContentPlaceHolder1_lblSecUses']/text()")
        if len(InAddition) > 0:
            MaxConcentration = InAddition[0]
            MaxConcentration = MaxConcentration[MaxConcentration.find('concentration of') + 16:].strip()
            MaxConcentration = MaxConcentration[: MaxConcentration.find(' ')]

        CropTable2 = parsed.xpath("//table[@id='ContentPlaceHolder1_tblSecUses']//tbody//tr")

        for i in range(len(CropTable2)):
            more = CropTable2[i][0].text.split(',')
            muchmore = more
            for m in more:
                if ' and ' in m:
                    muchmore.remove(m)
                    muchmore = muchmore + m.split(' and ')
            for m in muchmore:
                orddict = OrderedDict()
                orddict['Crop'] = m.strip()
                orddict['Pesticide Category'] = 'All approved pesticides applied up to their full approved rate'
                orddict['Maximum Concentration'] = MaxConcentration
                orddict['Maximum number of treatments'] = ''
                orddict['Latest time of application'] = CropTable2[i][1].text.strip()
                dictionary['CropInfo'].append(orddict)

        OPTable = parsed.xpath("//table[@id='ContentPlaceHolder1_tblOPPhrase']/tr")
        dictionary['Operator Instructions'] = []
        for i in range(len(OPTable)):
            dictionary['Operator Instructions'].append(OPTable[i][1].text)


        OSTable = parsed.xpath("//table[@id='ContentPlaceHolder1_tblOSRPhrase']/tr")
        dictionary['Specific Restrictions'] = []
        for i in range(len(OSTable)):
            dictionary['Specific Restrictions'].append(OSTable[i][1].text)


        dictionary.update({'List Entry - Document': parsed.xpath("//span[@id='ContentPlaceHolder1_lblListEntryNumber']//text()")[0].split('. ')[1]})


        os.chdir(self.path)


        try:
            os.chdir(self.path + '/adjuvants')
        except OSError as e:
            print("Error changing directory: {}".format(e))

        if 'Click on the icon to download' in r.text:
            # Download all adjuvant documents
            dlink = "https://secure.pesticides.gov.uk/adjuvants/DocumentCall.aspx?id=" + dictionary['List Entry - Document']
            r = self.s.get(dlink, stream=True)

            d = r.headers['content-disposition']
            file_extension = re.findall("filename=.+(\.\w+)", d)[0]

            if self.checkStatus(r, dlink):
                download = open(dictionary['List Entry - Document'] + file_extension, "wb")

                for chunk in r.iter_content(chunk_size=256):
                    if chunk:
                        download.write(chunk)


                time.sleep(0.2)

        set_flags = ['CropInfo', 'Formulation Substances']
        for f in set_flags:
            if not dictionary.get(f):
                dictionary['flags'].append('Adjuvant does not have {}'.format(f))

        if not dictionary['flags']:
            del dictionary['flags']

        return dictionary

    @staticmethod
    def send_to_ftp(filepath):
        import zipfile
        import pysftp

        def zipdir(path, ziph):
            # ziph is zipfile handle
            for root, dirs, files in os.walk(path):
                for file in files:
                    if 'zip' not in file:
                        ziph.write(os.path.join(root, file),
                                   os.path.relpath(os.path.join(root, file), os.path.join(path, '..')))

        # Create Zip file

        out_dir = '/'.join(filepath.split('/')[:-1])
        os.chdir(out_dir)
        filename = filepath.split('/')[-1]
        filename = '{}.zip'.format(filename)

        zipf = zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED)
        zipdir('{}/'.format(filepath), zipf)
        zipf.close()

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        SFTP_HOST = os.environ.get('SFTP_HOST', '81.134.32.164')
        SFTP_USER = os.environ.get('SFTP_USER', 'Daniel')
        SFTP_PASS = os.environ.get('SFTP_PASS', 'YYh7*(0^1o45B')
        SFTP_PORT = os.environ.get('SFTP_PORT', 2222)

        srv = pysftp.Connection(host=SFTP_HOST, username=SFTP_USER,
                                password=SFTP_PASS, port=SFTP_PORT,
                                cnopts=cnopts)

        with srv.cd('Pesticides'):
            srv.put(filename)

        # Closes the connection
        srv.close()

    @staticmethod
    def main():

        example_text = '''usage:
            python Pestice_Scraper.py -p 
            python Pestice_Scraper.py -p -u 
            python Pestice_Scraper.py -a 
        '''

        ap = argparse.ArgumentParser(description='Pesticide Scraper UK', epilog=example_text, formatter_class=argparse.RawDescriptionHelpFormatter)
        ap.add_argument("-p", action='store_true', dest='pesticide', help="scrape pesticide catalog")
        ap.add_argument("-e", action='store_true', dest='extensions', help="scrape extensions catalog")
        ap.add_argument("-a", action='store_true', dest='adjuvant', help="scrape adjuvant catalog")
        ap.add_argument("-u", action='store_true', dest='update', help="scrape changes for target option -p or -a")
        ap.add_argument("-s", action='store_true', dest='ftp', help="Send the information to the FTP")

        if len(sys.argv)==1:
            ap.print_help(sys.stderr)
            sys.exit(1)

        args = ap.parse_args()
        scraper = PesticideScraper()

        if args.pesticide:
            try:
                ini_p = 0

                if ini_p == 0:
                    pesticides = {}
                    substances = {}

                    # Initialize file
                    json.dump(pesticides, open(scraper.path + "/pesticide.json", "w"))
                    json.dump(substances, open(scraper.path + "/active_substances.json", "w"))
                else:
                    pesticides = json.load(open(scraper.path + "/pesticide.json", "r"))
                    substances = json.load(open(scraper.path + "/active_substances.json", "r"))

                # Perform daily update
                if args.update:
                    print("STARTING Pesticides AND Active Substances UPDATE...")

                    # Get new records link list
                    links = scraper.getPesticideLinks('New')
                    print(len(links), "Pesticide records to be extracted...")

                    # Get information from Details Page and Documents
                    for cont, link in enumerate(links):
                        print("\t" + str(cont) + "/" + str(len(links)))
                        try:
                            pesticide, substance = scraper.getPesticides(link['link'], update=True, notice_type=link['notice_type'], file_link=link['file_link'])
                        except Exception as e:
                            print(link, "ignored")
                            print(str(e))
                            continue

                        pesticides[pesticide['MAPP (Reg.) Number']] = pesticide

                        json.dump(pesticides, open(scraper.path + "/pesticide.json", "w"))

                        for name, s in substance.items():
                            if name not in substances:
                                substances[name] = s
                            else:
                                for d in substances[name]['documents']:
                                    try:
                                        if datetime.strptime(s['documents'][0]['date_of_issue'],
                                                             '%d/%m/%Y') > datetime.strptime(d['date_of_issue'],
                                                                                             '%d/%m/%Y'):
                                            substances[name]['expirations'] = s['expirations']
                                    except:
                                        pass

                                doc_ids = [d['document_id'] for d in substances[name]['documents']]
                                for doc in s['documents']:
                                    if doc['document_id'] not in doc_ids:
                                        substances[name]['documents'].append(doc)
                                        doc_ids.append(doc['document_id'])

                        json.dump(substances, open(scraper.path + "/active_substances.json", "w"))

                        time.sleep(0.4)

                    json.dump(pesticides, open(scraper.path + "/pesticide.json", "w"), indent=2)
                    json.dump(substances, open(scraper.path + "/active_substances.json", "w"), indent=2)

                # Perform entire extraction
                else:
                    print("STARTING Pesticides AND Active Substances ENTIRE EXTRACTION...")

                    # Get links list
                    links = scraper.getPesticideLinks('All')
                    print(len(links), "Pesticide records to be extracted...")

                    # Get information from Details Page and Documents
                    for cont, link in enumerate(links):

                        if cont < ini_p:
                            continue

                        print("\t" + str(cont) + "/" + str(len(links)))
                        try:
                            pesticide, substance = scraper.getPesticides(link)
                        except Exception as e:
                            print(link, "ignored")
                            print(str(e))
                            continue
                        pesticides[pesticide['MAPP (Reg.) Number']] = pesticide

                        shutil.copyfile(scraper.path + "/pesticide.json", scraper.path + "/pesticide_bckp.json")
                        json.dump(pesticides, open(scraper.path + "/pesticide.json", "w"))

                        for name, s in substance.items():
                            if name not in substances:
                                substances[name] = s
                            else:
                                for d in substances[name]['documents']:
                                    try:
                                        if datetime.strptime(s['documents'][0]['date_of_issue'],'%d/%m/%Y') > datetime.strptime(d['date_of_issue'],'%d/%m/%Y'):
                                            substances[name]['expirations'] = s['expirations']
                                    except:
                                        pass

                                doc_ids = [d['document_id'] for d in substances[name]['documents']]
                                for doc in s['documents']:
                                    if doc['document_id'] not in doc_ids:
                                        substances[name]['documents'].append(doc)
                                        doc_ids.append(doc['document_id'])

                        shutil.copyfile(scraper.path + "/active_substances.json", scraper.path + "/active_substances_bckp.json")
                        json.dump(substances, open(scraper.path + "/active_substances.json", "w"))

                        time.sleep(0.4)

                    print("Pesticides completed, converting to JSON")
                    json.dump(pesticides, open(scraper.path + "/pesticide.json", "w"), indent=2)
                    json.dump(substances, open(scraper.path + "/active_substances.json", "w"), indent=2)
                    print("Conversion succesffully")
            except Exception as e:
                print("It was not possible to complete pesticide extraction")
                traceback.print_exc()

        if args.extensions:
            try:
                # Perform daily update
                if args.update:
                    print("STARTING extensions of authorization UPDATE...")
                    extensions = scraper.getExtensions('New')
                    print(len(extensions), "Extensions of authorization extracted")

                    json.dump(extensions, open(scraper.path + "/extensions.json", "w"), indent=2)

                # Perform entire extraction
                else:
                    print("STARTING extensions of authorization ENTIRE EXTRACTION...")
                    extensions = scraper.getExtensions('All')
                    print(len(extensions), "Extensions of authorization extracted")

                    json.dump(extensions, open(scraper.path + "/extensions.json", "w"), indent=2)

                print("Done with extensions")
            except Exception as e:
                print("It was not possible to complete extension extraction")
                traceback.print_exc()

        if args.adjuvant:
            try:
                # Perform daily update
                if args.update:
                    print("STARTING Adjuvants UPDATE...")

                    # Get new records link list
                    additions, removals = scraper.getAdjuvantLinks('New')
                    adjuvants = {}

                    # Write removal records
                    print(len(removals), "Adjuvant removals were extracted")
                    if removals:
                        adjuvants = removals

                    # Get information from Details Page for addition records
                    print(len(additions), "Adjuvant additions to be extracted...")

                    for cont, link in enumerate(additions):
                        print("\t" + str(cont + 1) + "/" + str(len(additions)))
                        adjuvant = scraper.getAdjuvantInfo(link)
                        adjuvants[adjuvant['Number']] = adjuvant

                        json.dump(adjuvants, open(scraper.path + "/adjuvants.json", "w"), indent=2)
                        time.sleep(5)

                # Perform entire extraction
                else:
                    print("STARTING Adjuvants ENTIRE EXTRACTION...")

                    # Get new records link list
                    links = scraper.getAdjuvantLinks('All')
                    print(len(links), "Adjuvant records to be extracted...")
                    adjuvants = {}

                    # Get information from Details Page
                    for cont, link in enumerate(links):
                        print("\t" + str(cont) + "/" + str(len(links)))
                        adjuvant = scraper.getAdjuvantInfo(link)
                        adjuvants[adjuvant['Number']] = adjuvant

                        json.dump(adjuvants, open(scraper.path + "/adjuvants.json", "w"), indent=2)
                        time.sleep(5)

                print("Done with adjuvants")
            except Exception as e:
                print("It was not possible to complete adjuvants extraction")
                traceback.print_exc()
        if args.ftp:
            try:
                scraper.send_to_ftp(scraper.path)
            except Exception as e:
                print("It was not possible to send data to FTP")
                traceback.print_exc()


if __name__ == '__main__':
    PesticideScraper.main()

