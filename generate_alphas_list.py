import json

def read_fields_list(path):
	with open(path) as f:
		content = f.readlines()
	content = [x.strip() for x in content]
	return content

def read_json(path):
	with open(path) as f:
		return json.load(f)

def write_json(data, path):
	with open (path,'w') as f:
		json.dump({'alphas':data}, f)

def read_pattern(path):
	with open(path, 'r') as f:
		patterns = f.read()
	patterns_c = patterns.split('START')[1::]
	patterns_list = []
	for c in patterns_c:
		pattern_json = {"univid":c.split("univid:")[1].split("\n")[0].replace(' ',''),
						"optrunc":float(c.split("optrunc:")[1].split("\n")[0].replace(' ','')),
						"code":c.split("STOP")[0].replace('\n\n',''),
						"decay":int(c.split("decay:")[1].split("\n")[0].replace(' ','')),
						"region":c.split("region:")[1].split("\n")[0].replace(' ',''),
						"opneut":c.split("opneut:")[1].split("\n")[0].replace(' ',''),
						"parent":c.split("parent:")[1].split("\n")[0].replace(' ',''),
						"opcodetype":c.split("opcodetype:")[1].split("\n")[0].replace(' ',''),
						}
		patterns_list.append(pattern_json)
	return patterns_list

ratio_path = 'ratios/'
input_path = 'input/'
pattern_path = 'patterns/'

pattern_list = ['p2.txt']
for pattern_name in pattern_list:
	pattern_inside_list = read_pattern(pattern_path + pattern_name)
	ratio_list = read_fields_list(ratio_path + 'ratio.txt')
	alphas_list = []

	for pattern in pattern_inside_list:
		pattern_buff = pattern.copy()

		for ratio in ratio_list:
			pattern_buff = pattern.copy()
			try:
				pattern_buff['code'] = pattern['code'].replace('FIELD_1', ratio.split('/')[0]).replace('FIELD_2', ratio.split('/')[1])
			except:
				pattern_buff['code'] = pattern['code'].replace('FIELD_1', ratio.split('/')[0])

			alphas_list.append(pattern_buff)
	write_json(alphas_list, input_path + pattern_name[:-3] + 'json')