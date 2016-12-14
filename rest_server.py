import logging

from flask import Flask, make_response as _make_response
from xml.sax.saxutils import quoteattr

import borders.borders

app = Flask(__name__)


def make_response(ret, code):
    resp = _make_response(ret, code)
    resp.mimetype = 'text/xml; charset=utf-8'
    return resp


@app.route("/osm/granice/<terc>.osm", methods=["GET", ])
def get_borders(terc):
    resp = make_response(borders.borders.get_borders(terc), 200)
    resp.headers['Content-Disposition'] = 'attachment; filename={0}.osm'.format(terc)
    return resp


@app.errorhandler(Exception)
def report_exception(e):
    app.logger.error(e, exc_info=(type(e), e, e.__traceback__))
    return make_response(
        """<?xml version='1.0' encoding='UTF-8'?>
        <osm version="0.6" generator="import adresy merger.py">
            <node id="-1" lon="19" lat="52">
                <tag k="fixme" v=%s />
            </node>
        </osm>""" % quoteattr(repr(e)), 200)


if __name__ == '__main__':
    ADMINS = ['logi-osm@vink.pl']
    if not app.debug:
        from logging.handlers import SMTPHandler

        mail_handler = SMTPHandler('127.0.0.1',
                                   'server-error@vink.pl',
                                   ADMINS, 'OSM Rest-Server Failed')
        mail_handler.setLevel(logging.INFO)
        app.logger.addHandler(mail_handler)
    app.run(host='0.0.0.0', port=5002, debug=False)
