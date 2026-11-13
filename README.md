
# NYC Taxi Reproducible Data Pipeline

**รายวิชา CS341 – Data Engineering**  
**Submission 2: Reproducible Data Pipeline**

## 1. ภาพรวมโครงงาน (Project Overview)

โครงงานนี้เป็นการพัฒนาต่อยอดจากโครงงาน NYC Taxi Data Lifecycle ใน Submission 1 โดยนำกระบวนการจัดการข้อมูลมาพัฒนาเป็น Data Pipeline ที่สามารถทำงานซ้ำได้อย่างเป็นระบบ (Reproducible Data Pipeline)

โครงงานใช้ข้อมูลการเดินทางของรถแท็กซี่ในนิวยอร์ก (NYC Taxi Trip Data) ตั้งแต่เดือนมกราคมถึงมิถุนายน ค.ศ. 2024 ครอบคลุมบริการรถโดยสาร 4 ประเภท ได้แก่

- Yellow Taxi
- Green Taxi
- For-Hire Vehicles (FHV)
- High Volume For-Hire Vehicles (FHVHV)

ระบบแบ่งการทำงานออกเป็น 5 ขั้นตอนหลัก ดังนี้

1. **Ingestion (การนำเข้าข้อมูล)** – ดาวน์โหลดไฟล์ข้อมูลดิบในรูปแบบ Parquet และบันทึกข้อมูลการนำเข้าไว้ใน Ingestion Manifest
2. **Validation (การตรวจสอบข้อมูล)** – ตรวจสอบความสามารถในการอ่านไฟล์ โครงสร้าง Schema และขอบเขตช่วงวันที่ของข้อมูล
3. **Transformation (การแปลงข้อมูล)** – ทำความสะอาดและปรับมาตรฐานข้อมูล รวมถึงใช้กฎการตรวจสอบคุณภาพข้อมูล
4. **Integration (การรวมข้อมูล)** – รวมข้อมูลที่ผ่านการประมวลผลจากบริการรถทั้ง 4 ประเภทให้เป็นชุดข้อมูล Curated Dataset เดียว
5. **Analysis (การวิเคราะห์ข้อมูล)** – วิเคราะห์ปริมาณการใช้บริการรถแต่ละประเภทและสร้างรายงานในรูปแบบ CSV

โครงงานพัฒนาด้วยภาษา Python และใช้ DuckDB สำหรับประมวลผลและวิเคราะห์ข้อมูล โดยจัดเก็บข้อมูล Raw Dataset และ Curated Dataset ไว้บน Amazon S3

## 2. คำถามวิจัย (Research Question)

**บริการรถแท็กซี่ประเภทใดมีจำนวนเที่ยวการเดินทางมากที่สุดในช่วงครึ่งแรกของปี 2024 (มกราคม–มิถุนายน)?**

การวิเคราะห์จะเปรียบเทียบจำนวนเที่ยวการเดินทางทั้งหมดของบริการทั้ง 4 ประเภท ได้แก่ Yellow Taxi, Green Taxi, FHV และ FHVHV รวมถึงวิเคราะห์จำนวนเที่ยวการเดินทางในแต่ละเดือน เพื่อศึกษารูปแบบการใช้บริการตลอดช่วงเวลาที่กำหนด


## 3. สถาปัตยกรรมระบบ (System Architecture)

ระบบถูกออกแบบให้ทำงานเป็นลำดับขั้นตอน ตั้งแต่การนำเข้าข้อมูลดิบจนถึงการวิเคราะห์และสร้างรายงาน โดยใช้ Python เป็นเครื่องมือหลักในการควบคุมการทำงานของ Pipeline และใช้ DuckDB สำหรับประมวลผลข้อมูลในรูปแบบ Parquet

![แผนภาพสถาปัตยกรรมระบบ NYC Taxi Reproducible Data Pipeline](diagrams/pipeline_architecture.png)


### 3.1 องค์ประกอบของระบบ

- **Data Source:** ข้อมูล NYC Taxi Trip Data จากเว็บไซต์ NYC Taxi & Limousine Commission (TLC)
- **Local Data Storage:** จัดเก็บข้อมูล Raw Dataset และ Processed Dataset ในเครื่องระหว่างการประมวลผล
- **Processing Engine:** Python ใช้ควบคุมลำดับการทำงานของ Pipeline และ DuckDB ใช้ตรวจสอบ แปลง รวม และวิเคราะห์ข้อมูล
- **Curated Dataset:** ชุดข้อมูลที่ผ่านการประมวลผลและรวมข้อมูลจากรถทั้ง 4 ประเภทไว้ในไฟล์ Parquet เดียว
- **Output Reports:** รายงานผลการวิเคราะห์ในรูปแบบ CSV
- **Cloud Storage:** Amazon S3 ใช้จัดเก็บ Raw Dataset และ Curated Dataset เพื่อสำรองและเข้าถึงข้อมูลผ่าน Cloud Storage

### 3.2 ลำดับการทำงานของระบบ

Pipeline ทำงานตามลำดับ 5 ขั้นตอน ได้แก่ Ingestion, Validation, Transformation, Integration และ Analysis โดยแต่ละขั้นตอนใช้ผลลัพธ์จากขั้นตอนก่อนหน้าเป็นข้อมูลนำเข้า

ข้อมูลดิบที่ดาวน์โหลดจาก TLC จะถูกจัดเก็บไว้ใน Local Storage ก่อนเข้าสู่กระบวนการตรวจสอบและแปลงข้อมูล จากนั้นจึงรวมเป็น Curated Dataset และนำไปวิเคราะห์เพื่อสร้างรายงาน

นอกจากนี้ Raw Dataset และ Curated Dataset ยังถูกจัดเก็บไว้บน Amazon S3 แยกตามโฟลเดอร์ `raw/` และ `curated/` โดยใช้ AWS CLI ในการอัปโหลดข้อมูล
