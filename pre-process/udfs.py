import math

# Return the CPT_VEC including only those CPT tuples with the top value in the vector or that include at least 20% of the total value in the vector
@outputSchema("out: bag{t: tuple(cpt: chararray, val: double)}")
def top_cpt(cpt_vec):
	d = dict((cpt,float(val)) for (cpt,val) in cpt_vec)
	maxval = max(d.values())
	threshold = sum(d.values()) * 0.2
	out = [(cpt,val) for (cpt,val) in cpt_vec if val==maxval or val > threshold]
	return out

def norm_vec(vec): 
	return math.sqrt(sum([x*x for x in vec]))

# Compute cosine similarity between the source NPI and each NPI in the destination bag. Return all NPIs that are similar above a threshold value
@outputSchema("out: bag{t: tuple(npi: chararray) }")
def similarNpi(npi1, cpt_vec1, npi_bag, threshold):
	outBag = []
	d1 = dict(((cpt,float(val)) for (cpt,val) in cpt_vec1))
	norm1 = math.sqrt(sum([v*v for (k,v) in cpt_vec1]))
	for (npi2,cpt_vec2) in npi_bag:
		if npi1>npi2:
			norm2 = math.sqrt(sum([v*v for (k,v) in cpt_vec2]))
			dot_product = sum(d1[k]*v for (k,v) in cpt_vec2 if k in d1)
			shared_cpts = len([k for (k,v) in cpt_vec2 if k in d1])
			cosine_val = dot_product / (norm1*norm2)
			if cosine_val > threshold and shared_cpts >= 2:
				outBag.append((npi2))
	return outBag


