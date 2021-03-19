class LoginEntry(object):

	def __init__(self, username=None, password=None, service='tcp', mechanism='PLAIN'):
		self._username = username
		self._password = password
		self._service = service
		self._mechanism = mechanism

	@property
	def username(self):
		return self._username

	@property
	def password(self):
		return self._password

	@property
	def service(self):
		return self._service

	@property
	def mechanism(self):
		return self._mechanism
