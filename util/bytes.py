from sys import version_info


def to_bytes(origin):
	if version_info.major == 3:
		return bytes(origin, 'utf-8') if isinstance(origin, str) else origin
	return bytes(origin) if isinstance(origin, str) else origin


def to_str(origin):
	if version_info.major == 3:
		return origin.decode('utf-8') if isinstance(origin, bytes) else origin
	return str(origin) if isinstance(origin, bytes) else origin
