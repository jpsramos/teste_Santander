# teste_Santander

Teste basedo em um arquivo de log. O arquivo de log segue o padrão **Web Server Access Log**, e cada linha representa uma requisição HTTP.

No seguinte formato, abaixo um print breve de sua estrutura:

![image](https://github.com/user-attachments/assets/10e73cbf-ccb2-441c-9b95-99328886a676)

O teste deve cobrir respostas utilizando pyspark para os seguintes desafios:

### **Desafio:**
1. **Identifique as 10 maiores origens de acesso (Client IP) por quantidade de acessos.**
2. **Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.**
3. **Qual a quantidade de Client IPs distintos?**
4. **Quantos dias de dados estão representados no arquivo?**
5. **Com base no tamanho (em bytes) do conteúdo das respostas, faça a seguinte análise:**
   - O volume total de dados retornado.
   - O maior volume de dados em uma única resposta.
   - O menor volume de dados em uma única resposta.
   - O volume médio de dados retornado.
   - *Dica:* Considere como os dados podem ser categorizados por tipo de resposta para realizar essas análises.
6. **Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?**
---

### **Deployment:**
**Opção 2: Databricks Community Edition**
1. **Criação da Conta e Configuração do Ambiente:**
   - Crie uma conta gratuita em [Databricks Community Edition](https://community.cloud.databricks.com/).
   - Importe o arquivo de log para o ambiente do Databricks (você pode fazer upload diretamente ou usar um caminho HTTP para acessá-lo).

2. **Notebook:**
   - Desenvolva a solução usando um notebook Databricks com o código em **Python** ou **Scala**.
   - Certifique-se de que o código está bem estruturado e documentado.

3. **Entrega:**
   - Inclua um link para o notebook Databricks no seu repositório GitHub ou adicione o código completo diretamente no repositório.
   - No arquivo `README.md`, inclua:
      - **Instruções de como configurar e executar o código no Databricks**.
---

### **Primeiro aceeo Databricks Community Edition**
   - Realize o cadastro no ambiente e registro seu email como uma conta válida.
   - Para proteção e segurança, ative o padrão multifator e recebe seu tokem no email para acesso, após inserir email e o password cadastrado.
Abaixo, interface de acesso, criei um diretório com o nome Santander e inseri nele o arquivo correspondente aos testes que realizei.

![image](https://github.com/user-attachments/assets/b58a7e02-ab6e-409c-be19-8784a6e40e55)


   - Para iniciar o desenvolvimento, dvemos criar um cluster com definições de versão do spark e recurso de hardware para processar o arquivo. O menú ao lado esquerdo tem a opção Computer, nela criamos o cluster para uso.

![image](https://github.com/user-attachments/assets/f4ea54c9-1ae5-4191-a0aa-c3323a9212ed)

Para o desenvolvimento do teste, defini a seguinte configuração:

![image](https://github.com/user-attachments/assets/45f1dd02-7870-491a-8410-193506944d1f)

   - Criado o cluster e ativo para uso, é necessário fazer upload do arquivo para o ambiente, podemos fazê-lo clicando na opção Catalog, no menú ao lado esquerdo:
![image](https://github.com/user-attachments/assets/de0523a7-6d58-462b-a84a-68276a14552f)

No botão acima, create table ao clicar, poderemos movimentar o arquivo para o ambinete. 
Minha tabela está criada, exibirei como podemos criá-la mais adiante através do código, essa interface viabiliza criá-la diretamente, ativando algumas opções para formatá-la com base em sua estrutura, se o separador é virgula ou ponto e vírgula, por exemplo, entre outras propriedades.
Ao clicar em Create Table a seguinte tela será exibida

![image](https://github.com/user-attachments/assets/46e61644-378f-4b31-a069-1f346b0e8758)

Nela poderemos mover o arquivo para o upload, ao abrir um janela do windows e localizar o arquivo em seu computador, no instante que move-se este para o centro dessa tela, o upload inici-se automaticamente.

![image](https://github.com/user-attachments/assets/5df85b1d-22df-444c-94ff-5dc2e05fec65)

Finalizado o upload, iniciaremos o desenvolvimento.

---

### **Desenvolvimento:**

Tenho por prática fazer alguns testes antes de inicar um desenvolvimento real:

Faço conexões de teste para visualizar o conteúdo do arquivo, crio alguns metodos para testes iniciais.

![image](https://github.com/user-attachments/assets/d75e6a3e-0cf9-4aa9-8327-0231eff18c35)

![image](https://github.com/user-attachments/assets/54e57d19-5d5c-467d-8dbb-bcf278fadea7)


![image](https://github.com/user-attachments/assets/3dddeb30-c7ff-4635-88f1-859dc6598396)

Para o teaste optei em criar um classe com o nome SantanderAccessLog para realizar a entrega.

![image](https://github.com/user-attachments/assets/c128dfe9-4a26-4e60-baa0-6146298b2625)

   - O método parse_log_line, tratp a trnsposição das informações para coluna, utilizando expressão regular e leio-o como dataframe para inicar o teste/desenvolvimento

![image](https://github.com/user-attachments/assets/307dc9be-32be-444b-89dc-a7325cec686a)

![image](https://github.com/user-attachments/assets/2df90e94-72eb-44cb-8a0c-bd16e5d189bd)


   - O método write_to_delta_table crio a tabela no catalogo do ambiente

![image](https://github.com/user-attachments/assets/0a648c46-3469-40db-af79-c41432e9e3a6)

   - O método exercise_1 corresponde ao desenvolvimento para realizar a operação para detectar a resposta desejada no teste, o mesmo acontecerá para os demais

![image](https://github.com/user-attachments/assets/0099c440-4a4c-4dea-8bd7-9bd530f9b4aa)


   - O método exercise_2

![image](https://github.com/user-attachments/assets/16fcfa07-78e4-45c9-913e-f3919141dbd7)


   - O método exercise_3

![image](https://github.com/user-attachments/assets/f63d148a-6401-4ca6-a601-6ad4ffd24f05)


   - O método exercise_4

![image](https://github.com/user-attachments/assets/62ce4e93-b38d-44c8-9c65-22a79dfd6417)


   - O método exercise_5

![image](https://github.com/user-attachments/assets/3880255d-1949-49de-9b43-1cbdf69dc157)


   - O método exercise_6

![image](https://github.com/user-attachments/assets/09bfc12f-054e-4e87-b35a-91541a85b9fe)




