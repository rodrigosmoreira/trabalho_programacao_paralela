#include <iostream>
#include <sstream>
#include <fstream>
#include <strstream>
#include <omp.h>
#include <string.h>
#include <stdbool.h>
#include <iterator>
#include <map>
#include <vector>

#define CAMINHO_ARQUIVO_CSV "pequeno.csv"
#define DELIMITER ","

using namespace std;

int main(int argc, char const* argv[]) {
    // Vai armazena cada linha do arquivo de entrada
    std::string linha;

    // vai armazena cada celula do arquivo de entrada
    std::string celula;

    // vai armazena o id e nome de cada coluna do arquivo de entrada
    map<int, std::string> colunas;

    // vai armazenar o id e nome de cada valor de cada coluna do arquivo de entrada
    std::vector<std::map<std::string, int>> valoresColunasId(50);

    // vai armazenar a quantidade de ocorrencias de todos os valores de cada coluna do arquivo de entrada
    //int ocorrenciaValoresColunas[50][1000] = {};
    vector <int> ocorrenciaValoresColunas[50];


    // vai armazenar o ultimo id gerado para um valor de cada coluna do arquivo de entrada
    int idsValoresColuna[50] = { 0 };
    //std::vector<int> idsValoresColuna[3000] = { 0 };

    // auxiliares
    int idColuna = 0, idValorColuna, indiceLinha = 0, quantidadeLinha, quantidadeColuna;

    // Abre o arquivo de entrada .csv
    std::ifstream arquivo;
    arquivo.open(CAMINHO_ARQUIVO_CSV);

    // caso não abra vai lancar uma excecao
    if (!arquivo.good()) {
        cerr << "Erro: Arquivo " << CAMINHO_ARQUIVO_CSV << " não pode ser aberto" << endl;
        exit(1);
    }

    std::ofstream myfile;

    // vai criar o arquivo onde vai ser armazenado o arquivo de entrada (dataset) codificado
    myfile.open("dataset_codificado.csv");

    // salvando no map colunas, todas as colunas do arquivo de entrada e atribuindo um id para cada coluna
    std::getline(arquivo, linha);
    std::istringstream linhaCsv(linha);
    while (std::getline(linhaCsv, celula, ','))
    {
        if (linhaCsv.tellg() == -1) {
            myfile << celula << "\n";
        }
        else {
            myfile << celula << ",";
        }

        colunas.insert(pair<int, std::string>(idColuna++, celula));
    }

    quantidadeColuna = idColuna;
    

    cout << "Arquivo dataset_codificado.csv criado\n";

    string linhaCodificada;

    // Lendo os valores do arquivo de entrada e guardando o numero de ocorrencias no atributo ocorrenciaValoresColunas e salvando o valor codificado no "dataset_codificado.csv"
    #pragma omp parallel private(linha, idColuna, celula, idValorColuna, linhaCodificada, idColuna)
    {
            while (std::getline(arquivo, linha)) {
                std::istringstream linhaCsv(linha);
                idColuna = 0;
                linhaCodificada = "";

                // vai ler coluna por coluna de forma sequencial de cada linha
                for (idColuna = 0; idColuna < quantidadeColuna; idColuna++)
                {
                    std::getline(linhaCsv, celula, ',');

                    // ponto que precisa ser centralizado onde vai ser gerado um id para cada valor da coluna ou retornar o id ja atribuido para um valor
                    #pragma omp critical 
                    {
                        map<std::string, int>::const_iterator pos = valoresColunasId[idColuna].find(celula);
                        if (pos == valoresColunasId[idColuna].end()) {
                            valoresColunasId[idColuna].insert(pair<std::string, int>(celula, idsValoresColuna[idColuna]));
                            idValorColuna = idsValoresColuna[idColuna]++;
                            ocorrenciaValoresColunas[idColuna].push_back(-1);
                        }
                        else {
                            idValorColuna = pos->second;
                        }
                    }

                    // vai incrementar a ocorrencia de um determinado valor em uma determinada ccoluna
                    #pragma omp atomic
                    ocorrenciaValoresColunas[idColuna].push_back(ocorrenciaValoresColunas[idColuna][idValorColuna]++);

                    // vai armazena o valor na linha que vai ser salva no arquivo "dataset_codificado.csv"
                    linhaCodificada += std::to_string(idValorColuna);
                    if (idColuna < quantidadeColuna - 1) {
                        linhaCodificada += ",";
                    }
                }
                myfile << linhaCodificada << "\n";

                indiceLinha++;
            }
    }

    // vai armazenar a quantidade de linhas
    quantidadeLinha = indiceLinha;

    myfile.close();
    cout << "Arquivo dataset_codificado.csv criado\n";
    arquivo.close();

    // vai imprimir todos os arquivos .csv contendo a ocorrencia dos valores em cada coluna, fazendo isso de forma paralela para cada coluna do arquivo de entrada
    map<int, std::string>::iterator colunaIterator;
    #pragma omp parallel private(colunaIterator, idColuna, valoresColunaIterator, idValorColuna)
    {
        #pragma omp for
        for (colunaIterator = colunas.begin(), idColuna = 0; colunaIterator != colunas.end(); ++colunaIterator, idColuna++) {

            std::ofstream myfile;

            // vai criar o arquivo para armazenar as ocorrencias de cada valor em uma determinada coluna
            myfile.open("ocorrencias_coluna_" + colunaIterator->second + ".csv");
            myfile << colunaIterator->second << ",OCOR\n";

            map<std::string, int>::iterator valoresColunaIterator;

            for (valoresColunaIterator = valoresColunasId[idColuna].begin(), idValorColuna = 0; valoresColunaIterator != valoresColunasId[idColuna].end(); ++valoresColunaIterator, idValorColuna++) {
                myfile << valoresColunaIterator->first << "," << ocorrenciaValoresColunas[idColuna][idValorColuna] << "\n";
            }

            myfile.close();
            cout << "Arquivo ocorrencias_coluna_" + colunaIterator->second + ".csv criado\n";
        }
    }

    cout << "\nFinalizado voce pode encontrar os csv do resultado dentro da pasta do projeto\n";

    return 0;
}