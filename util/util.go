package util

func Page(page, pageSize int32, total int) (start int, end int) {
	p, size := int(page), int(pageSize)
	if page == 0 || pageSize == 0 || total == 0{
		return
	}
	start = (p - 1) * size
	end = start + size
	if start >= total {
		return 0, 0
	}
	if end > total {
		end = total
	}
	return
}


