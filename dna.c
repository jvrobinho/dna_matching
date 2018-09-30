#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"

// MAX char table (ASCII)
#define MAX 256

//Copies a slice from the original string "og_string"
//to the return variable "chunk",
//starting from "begin" and stopping at "end"
char * chunk_string(char * og_string, int begin, int end){	

	//Fail conditions
	if(!og_string) return NULL; //Empty string
	if(begin < 0 || end < 0) return NULL; //Negative indexes
	if(begin > end) return NULL; //Maybe invert "end" and "begin"?

    int size = (end - begin); //Result string's  size
	char * chunk = (char*) malloc(sizeof(char) * (size)); //Result string
    
	//Copy slice
    int i = 0;
    while (begin<=end){
		chunk[i] = og_string[begin]; //Copia primeiro char para 
        i++;
        begin++;
	}
	
	return chunk;
}





// Boyers-Moore-Hospool-Sunday algorithm for string matching
int bmhs(char *string, int n, char *substr, int m) {

	int d[MAX];
	int i, j, k;

	// pre-processing
	for (j = 0; j < MAX; j++)
		d[j] = m + 1;
	for (j = 0; j < m; j++)
		d[(int) substr[j]] = m - j;

	// searching
	i = m - 1;
	while (i < n) {
		k = i;
		j = m - 1;
		while ((j >= 0) && (string[k] == substr[j])) {
			j--;
			k--;
		}
		if (j < 0)
			return k + 1;
		i = i + d[(int) string[i + 1]];
	}

	return -1;
}

FILE *fdatabase, *fquery, *fout;

void openfiles() {

	fdatabase = fopen("dna.in", "r+");
	if (fdatabase == NULL) {
		perror("dna.in");
		exit(EXIT_FAILURE);
	}

	fquery = fopen("query.in", "r");
	if (fquery == NULL) {
		perror("query.in");
		exit(EXIT_FAILURE);
	}

	fout = fopen("dna.out", "w");
	if (fout == NULL) {
		perror("fout");
		exit(EXIT_FAILURE);
	}

}

void closefiles() {
	fflush(fdatabase);
	fclose(fdatabase);

	fflush(fquery);
	fclose(fquery);

	fflush(fout);
	fclose(fout);
}

void remove_eol(char *line) {
	int i = strlen(line) - 1;
	while (line[i] == '\n' || line[i] == '\r') {
		line[i] = 0;
		i--;
	}
}

char *bases;
char *str;
char *chunk;

int main(int argc, char **argv) {

	bases = (char*) malloc(sizeof(char) * 1000001);
	if (bases == NULL) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	str = (char*) malloc(sizeof(char) * 1000001);
	if (str == NULL) {
		perror("malloc str");
		exit(EXIT_FAILURE);
	}

	//INICIALIZAÇÃO(note que algumas dessas variaveis talvez nao sejam usadas em todos os processos(filhos talvez nao usem algumas...) )
	int myRank; // o ranking do processo atual 
	int source; // a fonte da mensagems recebida
	int tag = 50; // uma tag para um "broadcast particular"
	int tarefas = 4; // TESTE
	int origem;
	MPI_Status status; // status
	MPI_Init(&argc, &argv); // inicialização
	MPI_Comm_rank(MPI_COMM_WORLD, &myRank); // criação do ranking

	char desc_dna[100], desc_query[100];
	char line[100];
	int i, found, result,total,tamanhosub,contq,contc;

	// é o processo mestre, deve ler os arquivos e repassar strings para os filhos	
	if(myRank == 0)
	{
		openfiles();
		fgets(desc_query, 100, fquery); // le uma linha de fquery de até 100-1 characteres(DESCRIÇÃO DA QUERY)		
		remove_eol(desc_query);	
		//loop das query's (até o fim do arquivo)

		while (!feof(fquery)) 
		{
			contq = 1;
			for(i=0;i< tarefas;i++)
			{

			   MPI_Send(&contq,1,MPI_INT,i,tag,MPI_COMM_WORLD);
			}			
			//escrevendo a descrição da query
			fprintf(fout, "%s\n", desc_query);
			// read query string
			fgets(line, 100, fquery);
			remove_eol(line);
			//printf("antes do loop: %s \n",line);
			str[0] = 0;
			i = 0;

			do {
			//printf("antes de concatenar a sring procurada: %s \n",str);			
			strcat(str + i, line);
			//printf("depois: %s \n",str);
			if (fgets(line, 100, fquery) == NULL)
				break;
			remove_eol(line);
			//printf("depois remove_eol (next query): %s \n",line);
			i += 80;
			} while (line[0] != '>'); 
			printf("**********************************************************\n");
			printf("Query da vez: %s \n",str);
			printf("**********************************************************\n");
			//deve enviar str(a query) para todos os outros processos(por razoes de testes por enquanto to considerando 4 procesos)
			for(i=1; i < tarefas; i++)
			{
				MPI_Send(str, strlen(str)+1, MPI_CHAR, i, tag, MPI_COMM_WORLD);
			}

			strcpy(desc_query, line); // prepara a proxima query salvando na variavel de descrição
			
			// read database and search
			found = 0;
			fseek(fdatabase, 0, SEEK_SET); // ajusta para o começo do arquivo
			fgets(line, 100, fdatabase); // pega a 1a linha (descrição do genoma)
			remove_eol(line);
			//printf("READ DATA... \n");
			//printf("1ª linha(descricao  do genoma): %s \n",line);
			
			//loop do genoma
			while (!feof(fdatabase)) {
				contc = 1;
				for(i=0;i< tarefas;i++)
				{

			   		MPI_Send(&contc,1,MPI_INT,i,tag,MPI_COMM_WORLD);

				}
				//gravando a descrição do genoma
				strcpy(desc_dna, line);
				bases[0] = 0;
				i = 0;
			
				fgets(line, 100, fdatabase);
				remove_eol(line);
				//printf("genoma lido antes(tem limite de 100 caracteres, por isso tem a concatencao): %s \n",line);
				do {				
					strcat(bases + i, line); // evita ter que saber o tamanho da cadeia...
				//	printf("genoma depois(concatenado): %s \n",bases);
					if (fgets(line, 100, fdatabase) == NULL)
						break;
					remove_eol(line); // remove barra n's 
				//	printf("genoma next: %s	 \n",line);
					i += 80;
				} while (line[0] != '>'); // concatena similarmente ao outro loop
				
				tamanhosub = strlen(bases)/strlen(str); // calculo simples do tamanho do chunk (temporario)
				printf("**********************************************************\n");
				printf("genoma da vez: %s \n",bases);
				printf("**********************************************************\n");
				int inicio = 0;
				// envia para cada processo um pedaço do genoma
				for(i=1; i < tarefas; i++)
				{
									
					chunk = chunk_string(bases,inicio,(tamanhosub*i)-1);	
					MPI_Send(chunk, strlen(chunk)+1, MPI_CHAR, i, tag, MPI_COMM_WORLD);
					inicio = (tamanhosub*i);	
					// PROBLEMA: enviar o tamanho do chunk que é variavel			
				}
				for(i=1; i < tarefas; i++)
				{
					MPI_Recv(&result,1,MPI_INT,i,tag,MPI_COMM_WORLD,&status);
					if(result > 0)
					{
						found++;						
						fprintf(fout, "%s\n%d\n", desc_dna, result);
						break;	
					}
			
				}
			
				 
				
			
			}
			if (!found)
			fprintf(fout, "NOT FOUND\n");
			// terminou os genomas
			contc = 0;
			for(i=0;i< tarefas;i++)
			{

			   MPI_Send(&contc,1,MPI_INT,i,tag,MPI_COMM_WORLD);

			}

				
		}
		//terminou o arquivo
		contq = 0;
		for(i=0;i< tarefas;i++)
		{

			   MPI_Send(&contq,1,MPI_INT,i,tag,MPI_COMM_WORLD);
		}

		
		closefiles();
		free(str);
		free(bases);
			
	}

	else
	{
		printf("oi eu sou o %d um processo filho!\n",myRank);		
		MPI_Recv(&contq,1,MPI_INT,0,tag,MPI_COMM_WORLD,&status);
		 
		while(contq)
		{
			MPI_Recv(str,1000001,MPI_CHAR,MPI_ANY_SOURCE,tag,MPI_COMM_WORLD,&status);			
			printf("processo %d: recebi a query: %s \n",myRank,str);	
			MPI_Recv(&contc,1,MPI_INT,0,tag,MPI_COMM_WORLD,&status);					
			while(contc)
			{
				MPI_Recv(bases,1000001,MPI_CHAR,MPI_ANY_SOURCE,tag,MPI_COMM_WORLD,&status);
				printf("processo %d: recebi o chunk: %s \n",myRank,bases);
				result = bmhs(bases, strlen(bases), str, strlen(str));
				printf("processo %d: encontrei uma ocorrencia em: %d \n",myRank,result);
				MPI_Send(&result,1,MPI_INT,0,tag,MPI_COMM_WORLD);
				MPI_Recv(&contc,1,MPI_INT,0,tag,MPI_COMM_WORLD,&status);	
							
			}
			MPI_Recv(&contq,1,MPI_INT,0,tag,MPI_COMM_WORLD,&status);
			
			
			
		}		
		
	}
	MPI_Finalize();
	return EXIT_SUCCESS;

}
