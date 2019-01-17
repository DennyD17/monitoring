# -*- coding: utf-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from settings.settings import domain_login, domain_pass


def send_mes (text, to, files,subj='CTL check', cc=None, smtp_host="localhost", from_mes='fada11'):
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(text, 'html')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subj
    msg['From'] = from_mes
    msg['To'] = ','.join(to)
    if cc:
        msg['Cc'] = ','.join(cc)
    msg.attach(part1)
    msg.attach(part2)
    for f in files:
        with open(f, 'rb') as fil:
            part = MIMEApplication(fil.read(), Name=f.split('/')[-1])
        part['Content-Disposition'] = 'attachment; filename="%s"' % f.split('/')[-1]
        msg.attach(part)
    server = smtplib.SMTP(smtp_host)
    if smtp_host != "localhost":
        smtp_user = from_mes
        login = domain_login
        smtp_pass = domain_pass
        server.ehlo()
        server.starttls()
        server.login(login, smtp_pass)
    server.sendmail(from_mes, to, msg.as_string())
    server.quit()

    

table_template = """
                <table border='2' cellpadding='1' cellspacing='1'>
                    <thread>
                        <tr>
                            <th>WF ID</th>
                            <th>Category</th>
                            <th>Loading ID</th>
                            <th>Workflow</th>
                            <th>XID</th>
                            <th>Loading State</th>
                            <th>Start time</th>
                            <th>Sheduled ?</th>
                            <th>Engine</th>
                            <th>Query name</th>
                            <th>Exceptions</th>
                            <th>Incident</th>
                        </tr>
                    </thread>
                    <tbody>
                            %s
                    </tbody>
                </table>
"""
