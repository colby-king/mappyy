import csv


def write_csv(filename, data):

	fieldnames = data[0].keys()

	with open(filename, 'w') as csvfile:
		dw = csv.DictWriter(csvfile, fieldnames=fieldnames)
		dw.writeheader()
		dw.writerows(data)
