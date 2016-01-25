import logging
import CGIHTTPServer
  #handler = CGIHTTPServer.CGIHTTPRequestHandler

class WebCtrl(CGIHTTPServer.CGIHTTPRequestHandler):

    def __init__(self,hltd):
        self.logger = logging.getLogger(self.__class__.__name__)
        CGIHTTPServer.CGIHTTPRequestHandler.__init__(self)
        self.hltd = hltd

    def send_head(self):

        #if request is not cgi, handle internally
        if not super(CGIHTTPServer.CGIHTTPRequestHandler,self).is_cgi():
            path_pieces = self.path.split('/')[-1]
            if len(path_pieces)>=2:
              if path_pieces[-2]=='ctrl':
                  try:
                      #call hltd hook
                      self.hltd.webHandler(path_pieces[-1])
                  except Exception as ex:
                      self.logger.warning('Ctrl HTTP handler error: '+str(ex))
              else:
                  super(CGIHTTPServer.CGIHTTPRequestHandler,self).get_head()
             
        else:
            #call CGI handler
            super(CGIHTTPServer.CGIHTTPRequestHandler,self).get_head()
