import sendmail
from read_excel import get_birthday_date
import facility_email
from datetime import date
# addresses=['rakesh.chopkar@dell.com']
#message='test dell.com'
#sendmail.do_email(addresses, message, subject, sender=send_from, subtype='html')

def main():
    subject='Wish you a very Happy Birthday '
    send_from='rakesh.chopkar@dell.com'
    birthDayList = get_birthday_date()
    if birthDayList:
        for send_info in birthDayList:

            print '{} ------------sending to facilities-------------------'.format(date.today())
            f_body = 'Request for cube decoration of {} desk'.format(send_info[0])
            f_subject = 'Request for cube Decoration of {}'.format(send_info[0])
            f_addresses = ['punefacilityhelpdesk@emc.com']
            facility_email.do_email(f_addresses, f_body, f_subject, sender=send_from)
# 
            print '{} ----------------sending to employee--------------------'.format(date.today())
            name = send_info[0].split()[0]
            subject1 = subject + name
            addresses = [send_info[1]]
            print 'addresses-- ', addresses
            body = '<b><i><h1>Hi {}</h1></i></b><br><img src="cid:image1"><br>Regards,<br>UDS Family!'.format(name.capitalize())
            #print 'body--',body
            sendmail.do_email(addresses, body, subject1, True,
                              sender=send_from,
                              subtype='html')



if __name__ == '__main__':
    main()
#     f_body = 'checking for bcc and subject line for facility'
#     f_subject = 'Test for BCC content and facility'
#     #print 'f_subject--', f_subject1
#     f_addresses = ['rakesh.chopkar@dell.com']
#     send_from='rakesh.chopkar@dell.com'
#     facility_email.do_email(f_addresses, f_body, f_subject, sender=send_from)
