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


def do_email(recipients, body, subject, attachments=None, sender=None,
             subtype=None):
    if attachments is None:
        attachments = []
    if sender is None:
        sender = recipients[0]
    if subtype is None:
        subtype = 'plain'

    attachments = add_attachments(attachments)

    if attachments:
        msg = MIMEMultipart()
        msg.preamble = body
        for attachment in [body] + attachments:
            msg.attach(attachment)
    else:
        msg = MIMEText(body, _subtype=subtype, _charset='utf-8')

    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate()
    msg['Subject'] = subject
    msg['cc'] = 'rakesh.chopkar@dell.com'
    msg = msg.as_string()

    toAddress = recipients + ['rakesh.chopkar@dell.com']
    server = smtplib.SMTP(MAILSERVER, timeout=10)
    server.sendmail(sender, toAddress, msg)
    print 'sent to facility successfully'
    server.quit()
