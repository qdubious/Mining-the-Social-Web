# -*- coding: utf-8 -*-

import sys
import mailbox
import email
import quopri
import json
from BeautifulSoup import BeautifulSoup

MBOX = sys.argv[1]
OUT_FILE = sys.argv[2]


def cleanContent(msg):
    # Decode message from "quoted printable" format

    msg = quopri.decodestring(msg)

    # Strip out HTML tags, if any are present

    soup = BeautifulSoup(msg)
    return ''.join(soup.findAll(text=True))


def stripTags(text):
    import re
    scripts = re.compile(r'<script.*?/script>')
    css = re.compile(r'<style.*?/style>')
    tags = re.compile(r'<.*?>')

    text = scripts.sub('', text)
    text = css.sub('', text)
    text = tags.sub('', text)

    return text


def jsonifyMessage(msg):
    json_msg = {'parts': []}
    for (k, v) in msg.items():
        json_msg[k] = v.decode('utf-8', 'ignore')

    # The To, CC, and Bcc fields, if present, could have multiple items
    # Note that not all of these fields are necessarily defined

    for k in ['To', 'Cc', 'Bcc']:
        if not json_msg.get(k):
            continue
        json_msg[k] = json_msg[k].replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').decode('utf-8', 'ignore').split(',')

    try:
        for part in msg.walk():
            json_part = {}
            if part.get_content_maintype() == 'multipart':
                # todo: base64decode(content) and save to web server returning link for json object
                continue
            json_part['contentType'] = part.get_content_type()
            payload = part.get_payload(decode=False)
            content = ''
            if type(payload) is str:
                content = payload.decode('utf-8', 'ignore')
            elif type(payload) is list:
                content = '\n\n\n'.join([str(p).decode('utf-8', 'ignore') for p in payload])
            json_part['content'] = stripTags(cleanContent(content))

            json_msg['parts'].append(json_part)
    except Exception as e:
        sys.stderr.write('Skipping message - error encountered (%s)\n' % str(e))
    finally:
        return json_msg


# There's a lot of data to process, so use a generator to do it. See http://wiki.python.org/moin/Generators
# Using a generator requires a trivial custom encoder be passed to json for serialization of objects
class Encoder(json.JSONEncoder):
    def default(self, o): return list(o)


# The generator itself...
def gen_json_msgs(mb):
    while 1:
        msg = mb.next()
        if msg is None:
            break
        yield jsonifyMessage(msg)


mbox = mailbox.UnixMailbox(open(MBOX, 'rb'), email.message_from_file)
json.dump(gen_json_msgs(mbox), open(OUT_FILE, 'wb'), indent=4, cls=Encoder)
