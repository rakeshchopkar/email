#!/usr/bin/env python
"""
Inspired by:

https://raw.githubusercontent.com/yaneurabeya/scratch/master/bayonetta/scratch/scripts/python/sendmail.py
"""

import optparse
import os
import sys

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.Utils import formatdate
from email.MIMEImage import MIMEImage

import smtplib

MAILSERVER = 'mail.isilon.com'


def add_attachments(attachment_files):
    attachments = []
    for attachment_file in attachment_files:
        attachment = MIMEText(open(attachment_file, 'rb').read(),
                              _charset='utf-8')
        attachment.add_header('Content-Disposition', 'attachment',
                              filename=os.path.basename(attachment_file))
        attachments.append(attachment)
    return attachments


def do_email(recipients, body, subject, attachments, sender=None,
             subtype=None):
#     if attachments is None:
#         attachments = []
    if sender is None:
        sender = recipients[0]
    if subtype is None:
        subtype = 'plain'

    msg = MIMEMultipart()
    msg.preamble = 'image test'

    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate()
    msg['Subject'] = subject
    msg['cc'] = 'list.uds.pune@emc.com'

    if attachments:
        msgAlternative = MIMEMultipart('alternative')
        msg.attach(msgAlternative)

        msgText = MIMEText('This is the alternative plain text message.')
        msgAlternative.attach(msgText)

        msgText = MIMEText(body, 'html')
        msgAlternative.attach(msgText)

        fp = open('/home/rakesh/email/test.jpg', 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close()

        # Define the image's ID as referenced above
        msgImage.add_header('Content-ID', '<image1>')
        msg.attach(msgImage)
    else:
        msg = MIMEText(body, _subtype=subtype, _charset='utf-8')
    msg = msg.as_string()

    toAddress = recipients + ['list.uds.pune@emc.com']
    print 'sending mail to -- ', toAddress
    server = smtplib.SMTP(MAILSERVER, timeout=10)
    server.sendmail(sender, toAddress, msg)
    print 'mail sent successfully'
    print '....................................................................................'
    server.quit()


def main(argv=None):
    parser = optparse.OptionParser()
    parser.add_option('--attachment', action='append', default=[],
                      dest='attachments')
    parser.add_option('--from', default=None, dest='sender')
    parser.add_option('--to', action='append', default=[], dest='recipients')
    parser.add_option('--subtype', default='plain')
    parser.add_option('--subject', default='')

    opts, args = parser.parse_args()

    if not opts.recipients:
        parser.error('You must specify at lease one recipient with --to')

    body = sys.stdin.read()

    do_email(opts.recipients, body, opts.subject,
             attachments=opts.attachments, sender=opts.sender,
             subtype=opts.subtype)


if __name__ == '__main__':
    main()
