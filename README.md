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

   - O método parse_log_line, trata a transposição das informações para coluna, utilizando expressão regular e leio-o como dataframe para inicar o teste/desenvolvimento

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

---

### **Print Respostas:**

![image](https://github.com/user-attachments/assets/89a57353-7ce9-46d7-892c-fae3937a6270)


![image](https://github.com/user-attachments/assets/41cda744-edca-4c39-8c96-c0d92a61eff3)


![image](https://github.com/user-attachments/assets/265c521a-fb73-44f6-af24-11b158ad011f)

---


### **Recomendações - Boas práticas:**


1. Organização e Estrutura
Use Workspaces de forma organizada: Estruture seus notebooks e projetos em pastas lógicas para facilitar a navegação e colaboração.
Nomeie notebooks e clusters de forma descritiva: Use nomes que reflitam o propósito do notebook ou cluster, como ETL_Sales_Data ou Cluster_Analytics_Team.
Versionamento de código: Integre o Databricks com sistemas de controle de versão, como Git, para rastrear alterações no código.

3. Configuração de Clusters
Escolha o tipo de cluster adequado: Use clusters de propósito geral para desenvolvimento e clusters otimizados para produção.
Dimensionamento automático (Auto-scaling): Habilite o auto-scaling para ajustar automaticamente os recursos do cluster com base na carga de trabalho.
Desligamento automático: Configure o desligamento automático para evitar custos desnecessários quando o cluster estiver ocioso.
Use clusters compartilhados com cuidado: Para evitar conflitos, use clusters dedicados para tarefas críticas.

4. Segurança
Gerencie permissões: Configure permissões no nível do workspace, cluster e notebook para controlar o acesso.
Use tokens de acesso pessoal (PAT): Para autenticação segura ao interagir com a API do Databricks.
Proteja dados sensíveis: Use criptografia para armazenar dados sensíveis e evite expor credenciais em notebooks.
Auditoria e monitoramento: Ative logs de auditoria para rastrear atividades no workspace.

5. Boas Práticas de Desenvolvimento
Modularize o código: Divida o código em funções ou notebooks reutilizáveis para facilitar a manutenção.
Use bibliotecas externas: Instale pacotes necessários diretamente no cluster ou use bibliotecas gerenciadas pelo Databricks.
Teste e valide o código: Execute testes em amostras de dados antes de processar grandes volumes.
Documente o código: Adicione comentários e use células Markdown para explicar o propósito do notebook.

6. Gerenciamento de Dados
Use Delta Lake: Para garantir transações ACID, versionamento de dados e melhor desempenho em pipelines de dados.
Particione dados: Para melhorar o desempenho de consultas em grandes conjuntos de dados.
Cache de dados: Use o cache para acelerar consultas repetidas em dados frequentemente acessados.
Gerencie tabelas e metadados: Use o metastore para registrar tabelas e facilitar o acesso.

7. Performance
Otimize consultas Spark: Use explain() para analisar planos de execução e identificar gargalos.
Evite ações desnecessárias: Minimize o uso de operações como collect() e show() em grandes conjuntos de dados.
Broadcast Join: Use joins de broadcast para conjuntos de dados pequenos e grandes.
Persistência de dados: Escolha o nível de persistência adequado (MEMORY_AND_DISK, DISK_ONLY, etc.) com base na carga de trabalho.

8. Colaboração
Use comentários em notebooks: Colabore com sua equipe adicionando comentários diretamente nos notebooks.
Integre com ferramentas externas: Conecte o Databricks a ferramentas como Slack ou Jira para notificações e rastreamento de tarefas.
Compartilhe notebooks: Use links compartilháveis ou exporte notebooks para facilitar a colaboração.

9. Monitoramento e Depuração
Use o Spark UI: Monitore jobs, estágios e tarefas para identificar problemas de desempenho.
Logs estruturados: Configure logs estruturados para rastrear erros e eventos importantes.
Alertas e métricas: Configure alertas para monitorar o desempenho e o uso de recursos.

10. Custos
Gerencie custos de clusters: Use clusters de baixo custo para desenvolvimento e clusters otimizados para produção.
Armazenamento eficiente: Compacte e otimize arquivos para reduzir custos de armazenamento.
Monitore o uso: Use relatórios de uso do Databricks para identificar áreas de otimização de custos.

11. Integração com Outras Ferramentas
Conecte-se a provedores de nuvem: Use integrações nativas com AWS, Azure ou GCP para acessar dados armazenados na nuvem.
Integre com ferramentas de BI: Conecte o Databricks a ferramentas como Tableau ou Power BI para visualização de dados.
Automatize pipelines: Use ferramentas como Apache Airflow ou Databricks Workflows para orquestrar pipelines de dados.

