/* 
 * Abstract: TEA C Module for Python
 */

#include <Python.h>

void encipher(unsigned int *v, unsigned int *k) 
{
	unsigned int y=v[0], z=v[1], sum=0, i=0;
	unsigned int k0=k[0], k1=k[1], k2=k[2], k3=k[3];
	unsigned int delta=0x9e3779b9;
	for (; i <16; i++)
	{
		sum += delta;
		y += ((z<<4) + k0) ^ (z + sum) ^ ((z>>5) + k1);
		z += ((y<<4) + k2) ^ (y + sum) ^ ((y>>5) + k3);
	}
	v[0]=y;
	v[1]=z;
}

void decipher(unsigned int *v, unsigned int *k) 
{
	unsigned int y=v[0], z=v[1], sum=0xC6EF3720, i=0;
	unsigned int k0=k[0], k1=k[1], k2=k[2], k3=k[3];
	unsigned int delta=0x9e3779b9;                   
	sum = delta<<4;
	for (; i<16; i++)
	{ 
		z -= ((y<<4) + k2) ^ (y + sum) ^ ((y>>5) + k3);
		y -= ((z<<4) + k0) ^ (z + sum) ^ ((z>>5) + k1);
		sum -= delta;
	}
	v[0]=y;
	v[1]=z;
}

void xor(unsigned int *a, unsigned int *b, unsigned int *out) 
{
	unsigned int a0=a[0], a1=a[1], b0=b[0], b1=b[1];
	out[0] = (a0^b0)&0xffffffff;
	out[1] = (a1^b1)&0xffffffff;
}

char * tea_encrypt(const char *v, const int vlen, const char *k, int *olen) 
{
	unsigned char filln = 8 - vlen%8;
	if ( 8 == filln)
		filln = 0;
	int destlen = filln+vlen+8;
	char* vp = (char*)malloc(destlen);
	memset(vp,0,destlen);
	memcpy(vp,&filln,1);
	memcpy(vp+1+filln,v,vlen);
	char ao[8],o[8];
	char ro[8] = "\0\0\0\0\0\0\0\0";
	char to[8] = "\0\0\0\0\0\0\0\0";
	char* rp = (char*)malloc(destlen);
	int i;
	for (i=0; i<destlen;)
	{
		xor((unsigned int *)(vp+i),(unsigned int *)ro,(unsigned int *)ao);
		memcpy(o,ao,8);
		encipher((unsigned int *)o,(unsigned int *)k);
		xor((unsigned int *)o,(unsigned int *)to,(unsigned int *)ro);
		memcpy(to,ao,8);
		memcpy(rp+i,ro,8);
		i += 8;
	}
	free(vp);
	*olen = destlen;
	return rp;
}

char* tea_decrypt(const char *v, const int vlen, const char *k, int *olen) 
{
	char xplain[8],crypt[8],x[9];
	memset(xplain,0,8);
	memset(x,0,9);
	memcpy(xplain,v,8);
	decipher((unsigned int*)xplain,(unsigned int*)k);
	int pos = (int)(xplain[0]&0x07)+1;
	char* rp = (char*)malloc(vlen);
	memset(rp,0,vlen);
	memcpy(rp,xplain+pos,8-pos);
	memcpy(crypt,v,8);
	int i;
	for (i=8; i<vlen;)
	{
		xor((unsigned int*)(v+i),(unsigned int*)xplain,(unsigned int*)x);
		decipher((unsigned int*)x,(unsigned int*)k);
		memcpy(xplain,x,8);
		xor((unsigned int*)x,(unsigned int*)crypt,(unsigned int*)x);
		memcpy(crypt,v+i,8);
		memcpy(rp+i-pos,x,8);
		i += 8;
	}
	*olen = vlen - pos - 7;
	return rp;
}

static PyObject * py_encrypt(PyObject *self, PyObject *args)
{
	char *v,*k;
	int vlen;
	if (! PyArg_Parse(args, "(s#s)", &v, &vlen, &k))
	{
		return NULL;
	}
	else
	{
		int elen;
		char *en = tea_encrypt(v,vlen,k,&elen);
		PyObject* p = Py_BuildValue("s#", en, elen);
		free(en);
		return p;
	}
}

static PyObject * py_decrypt(PyObject *self, PyObject *args)
{
	char *v,*k;
	int vlen;
	if (! PyArg_Parse(args, "(s#s)", &v, &vlen, &k))
	{
		return NULL;
	}
	else
	{
		int plen;
		char *de = tea_decrypt(v,vlen,k,&plen);
		PyObject* p = Py_BuildValue("s#", de, plen);
		free(de);
		return p;
	}
}

static struct PyMethodDef methods[] =
{
	{"encrypt", py_encrypt, 1, "encrypt(v,k), return encypted"},
	{"decrypt", py_decrypt, 1, "decrypt(v,k), return decypted"},
	{NULL, NULL, 0, NULL}         
};

PyMODINIT_FUNC initmytea(void)
{    
	(void)Py_InitModule("mytea", methods);
}

/*
#include <stdio.h>
int main(){
	char v[] = "hello,man,liangnc,abcdefghijk&*^%$#@!~)(_+|}{:'/>><:哈哈";
	char k[] = "3987423847923742390231212";
	encipher((unsigned int*)v,(unsigned int*)k);
	decipher((unsigned int*)v,(unsigned int*)k);
	char a[] = "12345678";
	char b[] = "98711213";
	char c[8],d[8];
	xor((unsigned int*)a,(unsigned int*)b,(unsigned int*)c);
	xor((unsigned int*)b,(unsigned int*)c,(unsigned int*)d);
	int elen,plen;
	printf("encrypting: %d->%s\n",strlen(v),v);
	char *en = tea_encrypt(v,strlen(v),k,&elen);
	printf("encrypted: %d\n",elen);
	char *de = tea_decrypt(en,elen,k,&plen);
	printf("decrypting: %d->%s\n",plen,de);
	free(en);
	free(de);
}
*/

