# manipulate directories and files
import os
import glob

# progress bar
# from tqdm import tqdm

# protect concurrent code 
import asyncio

# download from url 
import aiohttp


#progress bar
from tqdm import tqdm


filecounter = 0
semaphore = asyncio.Semaphore(10)

def write_to_file(filename, content):
	"""
	Write data to disk
	"""
	f = open(filename, 'wb')
	f.write(content)
	f.close()


# @asyncio.coroutine
# def get(*args, **kwargs):
# 	"""
# 	A coroutine to perform get requests
# 	"""
# 	response = yield from aiohttp.request('GET', *args, **kwargs)
# 	# return (yield from response.text())
# 	return (yield from response.read())

@asyncio.coroutine
def get(*args, **kwargs):
	"""
	A coroutine to perform get requests
	"""
	session = response = None
	try:
		session = aiohttp.ClientSession()
		response = yield from session.get(*args, **kwargs)
		body = yield from response.read()
	except asyncio.CancelledError:
		raise
	except aiohttp.ClientError:
		# log...
		pass
	else:
		return body
	finally:
		if response is not None:
			response.close()
		if session is not None:
			session.close()




@asyncio.coroutine
def download_file(url, output_dir):
	"""
	Download  a single file in a manner protected by a semaphore
	"""
	with (yield from semaphore):
		content = yield from asyncio.async(get(url))
		global filecounter
		filecounter += 1
		extension = os.path.splitext(url)[1]
		filename = '{}/{}{}'.format(output_dir, filecounter, extension)

		write_to_file(filename, content)


@asyncio.coroutine
def wait_with_progressbar(coros):
	for f in tqdm(asyncio.as_completed(coros), unit='B', unit_scale=True,  miniters=1, total=len(coros)):
		yield from f



def extract_dataset(json_description_dataset, output_dir):
	"""
	Download an extract a dataset specified by a json object
	:param json_description_dataset: json object describing the database
	"""
	global filecounter
	filecounter = 0

	if os.path.exists(output_dir):
		files_path_jpg = output_dir + "/*.jpg"
		files_path_png = output_dir + "/*.png"
		images = glob.glob(files_path_jpg)
		images.extend(glob.glob(files_path_png))

		if len(images) == len(json_description_dataset):
			print('Images from json already downloaded')
			return None
	else:
		os.makedirs(output_dir)

	# Avoid too many requests(coroutines) the same time.
	# Let's limit them by setting semaphores (simultaneous requests)
	# global semaphore
	# semaphore = asyncio.Semaphore(10)

	images = [obj["source"] for obj in json_description_dataset]

	corountines = [download_file(url, output_dir) for url in images]
	# loop = asyncio.new_event_loop()

	# asyncio.set_event_loop(asyncio.new_event_loop())
	loop = asyncio.get_event_loop()
	loop.run_until_complete(wait_with_progressbar(corountines))
	# loop.close()