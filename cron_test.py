import facility_email
print '-------------------sending to facilities-------------------'
f_body = 'Requesting you to please decorate '
f_subject = 'Request for Birthday Decoration Today of'
f_addresses = ['rakesh.chopkar@Dell.com']
send_from='rakesh.chopkar@dell.com'
facility_email.do_email(f_addresses, f_body, f_subject, sender=send_from)